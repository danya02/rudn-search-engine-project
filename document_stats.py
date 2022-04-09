from functools import lru_cache
import string
import time
import re
from typing import List, Optional, Tuple

from numpy import insert

from database import *
from nltk.stem import SnowballStemmer

import uuid

import math

def filter_alnum(term: str) -> str:
    """
    Returns a string with only alphanumeric characters.
    """
    return ''.join(c for c in term if c.isalnum())

@lru_cache(maxsize=1000000)
def get_natural_term(term: str) -> Optional[NaturalTerm]:
    """
    Returns the natural term database model for the given word.

    If the term is not in the database, it will be added.
    If the term contains no alphanumeric characters, return None.
    """
    term = term.lower()
    term = filter_alnum(term)
    if not term:
        return None
    record, _ = NaturalTerm.get_or_create(name=term)
    return record

# NLTK snowball tokenizer
stemmer = SnowballStemmer('english')


def term_stem(term: str) -> str:
    """
    Returns the stem of the given term.
    """

    return stemmer.stem(term)

@lru_cache(maxsize=100000)
def normalize_term(natural_term: NaturalTerm) -> NormalizedTerm:
    """
    Turns a natural term record into a normalized term record.

    If the normalized record already exists, it will be returned;
    if not, it will be created and returned.
    """

    if natural_term.normalized_form:
        return natural_term.normalized_form

    normalized_name = term_stem(natural_term.name)

    record, _ = NormalizedTerm.get_or_create(name=normalized_name)
    natural_term.normalized_form = record
    natural_term.save()
    return record
    

@db.atomic()
def reset_document_counters(document: Document):
    """
    Removes all the cached counters for this document.

    Does not use the document's `content`, because it may have changed.

    Use this when the document has changed, or when the document is being deleted.
    """

    # Select all the terms that were in the document.
    terms_in_document = DocumentTermPairCount.select(DocumentTermPairCount.term).where(
        DocumentTermPairCount.document == document,
        DocumentTermPairCount.count > 0
    )

    # For every term in the document, decrement the number of documents that contain it.
    DocumentsContainingTermCount.update(
        count=DocumentsContainingTermCount.count - 1,
        last_updated=now()
    ).where(
        DocumentsContainingTermCount.term.in_(terms_in_document)
    ).execute()

    # If any DocumentsContainingTermCount records now have a count of 0, delete them.
    DocumentsContainingTermCount.delete().where(
        DocumentsContainingTermCount.count == 0
    ).execute()

    # Reset all document-term counters for this document
    DocumentTermPairCount.delete().where(DocumentTermPairCount.document == document).execute()
    DocumentTotalTermCount.delete().where(DocumentTotalTermCount.document == document).execute()
    DocumentTermPosition.delete().where(DocumentTermPosition.document == document).execute()

def split_with_separator(content):
    words_and_seps = re.split("(\\s+)", content)
    words = []
    for ind, word_or_sep in enumerate(words_and_seps):
        # If the index is even, it's a word, otherwise a separator
        if ind % 2 == 0:
            words.append(word_or_sep)
        else:
            words[-1] = words[-1] + word_or_sep
    return words

def split_by_good_words(content):
    return split_with_separator(content)


@db.atomic()
def index_document(document: Document):
    """
    Indexes a document, removing all existing relevant indexes.

    This should be called when a document is created or updated.
    """


    # If the document was already in the database, reset its counters.
    # We will recreate them now.
    reset_document_counters(document)

    # Record terms that we have already seen.
    seen_terms = set()

    normalized_terms = []

    # Split the document's content into words.
    words = document.content.split()
    word_natural_term_names = [filter_alnum(word.lower()) for word in words]

    # Select all the natural terms used in the document.
    unique_natural_term_names = set(word_natural_term_names)
    natural_term_name_to_model = dict()
    # Iterate over chunks of the unique set, because there is a limit on the number of parameters in a query.
    for chunk in pw.chunked(unique_natural_term_names, 500):
        natural_term_name_to_model.update(
            {natural_term.name: natural_term for natural_term in NaturalTerm.select().where(NaturalTerm.name.in_(chunk))}
        )


    # For each natural term not in the database, create it.
    natural_terms_to_create = []
    for natural_term_name in unique_natural_term_names:
        if natural_term_name not in natural_term_name_to_model:
            natural_terms_to_create.append(NaturalTerm(name=natural_term_name))
    NaturalTerm.bulk_create(natural_terms_to_create)

    # Now get the new natural terms.
    for chunk in pw.chunked(unique_natural_term_names.difference(set(natural_term_name_to_model)), 500):
        natural_term_name_to_model.update(
            {natural_term.name: natural_term for natural_term in NaturalTerm.select().where(NaturalTerm.name.in_(chunk))}
        )
    
    # Finally, put the list of natural terms in order of the document.
    natural_terms = [natural_term_name_to_model[natural_term_name] for natural_term_name in word_natural_term_names]

    # For each natural term, get its corresponding normalized term.
    # Because there are fewer normalized terms than natural terms,
    # we rely on the LRU cache around normalize_term.
    normalized_terms = [normalize_term(natural_term) for natural_term in natural_terms]
    unique_normalized_terms = set(normalized_terms)

    # For each normalized term, record how many times it's found in the document.
    dtpc = [DocumentTermPairCount(document=document, term=term, count=normalized_terms.count(term)) for term in seen_terms]
    DocumentTermPairCount.bulk_create(dtpc)

    # If there were any terms which did not have a corresponding DocumentsContainingTermCount record,
    # (which means this is a new term first found in this document),
    # create one for them.

    
    new_terms = []
    existing_terms = DocumentsContainingTermCount.select().where(
        DocumentsContainingTermCount.term.in_(unique_normalized_terms)
    )
    existing_terms = set([i.term.id for i in existing_terms])
    for term in seen_terms:
        if term not in existing_terms:
            new_terms.append(term)

    new_dctc_records = [
        DocumentsContainingTermCount(term=natural_term, count=0, last_updated=now())
        for natural_term in new_terms
    ]

    DocumentsContainingTermCount.bulk_create(new_dctc_records)


    # For every term we found, increment the number of documents that contain it.
    DocumentsContainingTermCount.update(
        count=DocumentsContainingTermCount.count + 1,
        last_updated=now()
    ).where(
        DocumentsContainingTermCount.term.in_(seen_terms)
    ).execute()

    # Update the total term count for this document.
    total_terms = len(normalized_terms)
    DocumentTotalTermCount.create(
        count=total_terms,
        last_updated=now(),
        document=document
    )

    # Create the forward index
    term_positions = [
        DocumentTermPosition(
            document=document,
            term=term,
            position=i
        ) for i, term in enumerate(normalized_terms)
    ]
    #for t in term_positions:
    #    t.save(force_insert=True)  # somehow this is faster than the bulk_create line?!?!?
    #DocumentTermPosition.bulk_create(term_positions)
    
    # Actually none of these are particularly fast, which makes sense as there are a lot of rows to insert.
    # We will instead write it into a temporary plain text file, and then load it into the database.
    with open('/mnt/Data/document_term_position_wals/' + str(uuid.uuid4()) + '.txt', 'a') as f:
        for t in term_positions:
            f.write(str(t.document.id) + ' ' + str(t.term.id) + ' ' + str(t.position) + '\n')
        f.write('END\n')
    print("Create issued...")


@db.atomic()
def generate_documents_containing_term_count():
    """
    Generates the DocumentsContainingTermCount table,
    creating a row for each term and setting the count to zero.
    """
    DocumentsContainingTermCount.delete().execute()
    DocumentsContainingTermCount.insert_from(
        NormalizedTerm.select(
            NormalizedTerm.id,
            0, now()
        ), fields=[DocumentsContainingTermCount.term_id, DocumentsContainingTermCount.count, DocumentsContainingTermCount.last_updated]
    ).execute()

def generate_counts_single_query():
    """
    "To show the power of SQL queries, I sawed this database in half!"
    
    Generate the DocumentsContainingTermCount, DocumentTermPairCount,
    and DocumentTotalTermCount tables, using a single query each.
    """

    #DocumentsContainingTermCount.delete().execute()
    #DocumentTermPairCount.delete().execute()
    #DocumentTotalTermCount.delete().execute()



    # dtpc = DocumentTermPairCount.insert_from(
    #     DocumentTermPosition.select(
    #         DocumentTermPosition.document_id,
    #         DocumentTermPosition.term_id,
    #         pw.fn.Count(DocumentTermPosition.position),
    #         now()
    #     ).group_by(
    #         DocumentTermPosition.document_id,
    #         DocumentTermPosition.term_id
    #     ), fields=[
    #         DocumentTermPairCount.document_id,
    #         DocumentTermPairCount.term_id,
    #         DocumentTermPairCount.count,
    #         DocumentTermPairCount.last_updated
    #     ]
    # )
    # print(dtpc)
    # start = time.time()
    # dtpc.execute()
    # print("Executed in", time.time() - start)

#    subq = DocumentTermPosition.select()
#
#    dctc = DocumentsContainingTermCount.insert_from(
#        subq.select_from(
#            subq.c.term_id,
#            pw.fn.Count(
#                subq.c.document_id
#            ),
#            now()
#        ).distinct().group_by(subq.c.term_id), fields=[
#            DocumentsContainingTermCount.term_id,
#            DocumentsContainingTermCount.count,
#            DocumentsContainingTermCount.last_updated
#        ]
#    )
#    print(dctc)
#    start = time.time()
#    dctc.execute()
#    print("Executed in", time.time() - start)

    # # This query was too confusing for me, so I'm just going to do it manually.
    # with db.atomic():
    #     for term, count in DocumentTermPairCount.select(
    #             DocumentTermPairCount.term_id,
    #             pw.fn.Count(DocumentTermPairCount.document_id)
    #         ).group_by(DocumentTermPairCount.term_id).tuples():
    #         print(term, count)
            
    #         DocumentsContainingTermCount.create(
    #             term_id=term,
    #             count=count,
    #             last_updated=now()
    #         )


    dttc = DocumentTotalTermCount.insert_from(
        DocumentTermPosition.select(
            DocumentTermPosition.document_id,
            pw.fn.Count(DocumentTermPosition.document_id),
            now()
        ).group_by(
            DocumentTermPosition.document_id
        ), fields=[
            DocumentTotalTermCount.document_id,
            DocumentTotalTermCount.count,
            DocumentTotalTermCount.last_updated
        ]
    )
    print(dttc)
    start = time.time()
    dttc.execute()
    print("Executed in", time.time() - start)

