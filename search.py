from database import *
from flask import abort
import document_stats
import tf_idf_tools
import proximity_search

def run_search(query, algo):
    """
    Runs a search for the given query.
    Yields a list of (score, doc_id) tuples.
    """
    # Get the terms in the query and find their normal forms.
    terms = query.split()
    natural_terms = [document_stats.get_natural_term(term) for term in terms]

    normalized_terms = [document_stats.normalize_term(term) for term in natural_terms]

    if algo == 'tf-idf-cos':
        query = run_tf_idf_search_cosine(normalized_terms)
    elif algo == 'tf-idf-sum':
        query = run_tf_idf_search_sum(normalized_terms)
    elif algo == 'proximity':
        query = run_proximity_search(normalized_terms)
    else:
        abort(400, 'Unknown algorithm: {}'.format(algo))

    return query

def run_tf_idf_search_cosine(normalized_terms):
    yield from tf_idf_tools.find_document_scores(normalized_terms, True)

def run_tf_idf_search_sum(normalized_terms):
    yield from tf_idf_tools.find_document_scores(normalized_terms, False)


def run_proximity_search(normalized_terms):

    def documents_containing_all_terms():
        # loop over all documents that have the first term
        for dtpc in DocumentTermPairCount.select().where(DocumentTermPairCount.term == normalized_terms[0]):
            for term in normalized_terms[1:]:
                # if it doesn't have the any one of the other terms, break
                if not DocumentTermPairCount.get_or_none(document=dtpc.document, term=term):
                    break
            else:
                yield dtpc.document

    documents_with_terms = [DocumentTermPairCount.select().where(DocumentTermPairCount.term == term) for term in normalized_terms]
    document_counts = [i.count() for i in documents_with_terms]

    yield from proximity_search.score_subset_of_docs(normalized_terms, documents_containing_all_terms(), min(document_counts))