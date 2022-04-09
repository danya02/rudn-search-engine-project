from functools import lru_cache
import math
from typing import Dict, Generator, Iterable, List, Tuple
from xmlrpc.client import Boolean
from database import *


@lru_cache(maxsize=1000)
def get_term_idf(term: NormalizedTerm) -> float:
    """
    Returns the IDF for the given term.
    """

    # Get the number of documents that contain this term.
    # This should have been a subquery,
    # but apparently those cannot be inside a CAST,
    # so we compute it here.
    n_containing = DocumentsContainingTermCount.select(
        DocumentsContainingTermCount.count
    ).where(
        DocumentsContainingTermCount.term == term
    ).limit(1).scalar()

    if n_containing is None:
        return 0.0

    # Return the IDF for this term.
    # IDF = log(N / n_containing)

    doc_count = Document.select().count()
    return math.log(
        doc_count / n_containing
    )

def get_term_tf(term: NormalizedTerm, idf_fac: float = 1) -> pw.ModelSelect:
    """
    Returns a Select query
    which establishes a mapping between every document
    and TF(term, document).

    `idf_fac` is premultiplied into the result; by default it is 1,
    meaning this returns plain TFs. If the term's IDF is supplied,
    this will return the TF-IDF score instead (though it will still be called `tf`).

    Column names:

    document_id: the document ID
    tf: the TF for the given term in said document
    document_length: number of terms in the document
    term_count: number of times the given term appears in the document
    """

    # Select every document;
    # join to it the total number of terms in the document;
    # join to it the number of times our term appears in the document;
    # calculate the ratio between the two (casting the first to float to force float division);
    # finally select the document, the document length, the term count, and the ratio
    tfs = Document.select(
        Document.id.alias('document_id'),
        DocumentTotalTermCount.count.alias('document_length'),
        DocumentTermPairCount.count.alias('term_count'),
        ((
            pw.Cast(DocumentTermPairCount.count, "float") / DocumentTotalTermCount.count)*idf_fac
        ).alias('tf')
    ).join(DocumentTotalTermCount)\
          .switch(Document).join(DocumentTermPairCount)\
            .where(DocumentTermPairCount.term == term)\
            .group_by(Document.id).namedtuples()
    
    return tfs

def get_tf_idf() -> pw.ModelSelect:
    """
    Get the query that contains every pair of documents and terms,
    and their TF-IDF scores.

    To get the TF-IDF score for a term in a document,
    just add a WHERE clause to the query to filter by the document and term.
    If there is no such clause, this will require a full-table scan,
    with a few binary searches based off of that;
    with a WHERE clause, it requires no such scans.

    This also returns a CTE used inside this query, so that it can be bound later.
    """

    # Count the number of documents in the database.
    # This is used to calculate the IDF for each term.
    # I haven't been able to find a way to include this
    # as a subquery in the main query, so I'm just
    # doing it manually.
    count_documents = Document.select().count()

    # Subquery for the TF and IDF metrics separately, as well as the values that go
    # into both, but separately.
    # As before, this is because I wasn't able to figure out how to combine
    # the two expressions for TF and IDF without repeating them.
    tfs_and_idfs = Document.select(
        Document.id.alias('document_id'),
        NormalizedTerm.id.alias('term_id'),
        DocumentTotalTermCount.count.alias('document_length'),
        DocumentTermPairCount.count.alias('term_count'),
        DocumentsContainingTermCount.count.alias('n_containing'),
        (
            pw.Cast(DocumentTermPairCount.count, "float") / DocumentTotalTermCount.count
        ).alias('tf'),
        (
            pw.fn.Log(
                count_documents / pw.Cast(DocumentsContainingTermCount.count, 'float')
            )
        ).alias('idf'),
        (
            pw.Cast(DocumentTermPairCount.count, "float") / DocumentTotalTermCount.count\
                *
            pw.fn.Log(
                count_documents / pw.Cast(DocumentsContainingTermCount.count, 'float')
            )
        ).alias('tf_idf')
    ).join(NormalizedTerm, join_type=pw.JOIN.CROSS)\
    .switch(Document).join(DocumentTotalTermCount, on=(Document.id == DocumentTotalTermCount.document))\
    .switch(NormalizedTerm).join(DocumentsContainingTermCount, on=(NormalizedTerm.id == DocumentsContainingTermCount.term))\
    .switch(Document).join(DocumentTermPairCount, on=((DocumentTermPairCount.document == Document.id) & (DocumentTermPairCount.term==NormalizedTerm.id)))


    return tfs_and_idfs

def document_vector(document: Document) -> pw.ModelSelect:
    """
    Return the TF-IDF vector for the given document.

    Returns a ModelSelect, so it can be used in a subquery.
    """

    tfidfs = get_tf_idf()

    return tfidfs.select_from(
        tfidfs.c.term_id,
        tfidfs.c.tf_idf
    ).where(tfidfs.c.document_id == document.id)

def document_vectors(documents: Iterable[Document]) -> pw.ModelSelect:
    """
    Return the TF-IDF vector for the given document.

    Returns a ModelSelect, so it can be used in a subquery.
    """

    tfidfs = get_tf_idf()

    return tfidfs.select_from(
        tfidfs.c.document_id,
        tfidfs.c.term_id,
        tfidfs.c.tf_idf
    ).where(tfidfs.c.document_id << documents)


def query_vector(query: List[NormalizedTerm]) -> List[Tuple[NormalizedTerm, float]]:
    """
    Return the TF-IDF vector for the given list of terms (considered as a separate document).

    Returns a list of tuples, each containing a term and its TF-IDF score.
    """
    idf_list = [get_term_idf(term) for term in query]
    tf_list = [query.count(term) / len(query) for term in query]
    tf_idf_list = [tf * idf for tf, idf in zip(tf_list, idf_list)]
    tf_idf_tuples = [(term, tf_idf) for term, tf_idf in zip(query, tf_idf_list)]

    return tf_idf_tuples

def get_cosine_similarity(query_vector: Dict[NormalizedTerm, float], document_vector: pw.ModelSelect) -> float:
    """
    Get the cosine similarity between the query vector and the document vector.

    This is a score that represents the quality of this document as a response to this query.
    """

    # Get the magnitude of the query vector.
    query_magnitude = math.sqrt(sum([tf_idf ** 2 for _, tf_idf in query_vector.items()]))

    # Get the dot product of the query vector and the document vector.
    dot_product = 0.0
    document_vector_magnitude = 0.0

    if isinstance(document_vector, pw.ModelSelect):
        tuples = document_vector.tuples()
    else:
        tuples = document_vector

    for term, tf_idf in tuples:
        if term not in query_vector:
            continue
        if tf_idf is None: continue
        dot_product += tf_idf * query_vector[term]
        document_vector_magnitude += tf_idf ** 2

    document_magnitude = math.sqrt(document_vector_magnitude)

    if query_magnitude * document_magnitude == 0:
        return 0.0

    cosine_similarity = dot_product / (query_magnitude * document_magnitude)

    return cosine_similarity

def find_document_scores(query: List[NormalizedTerm], use_cosine: Boolean = True) -> Generator[Tuple[Document, float], None, None]:
    """
    Find the scores for every document that matches the given query.

    Yields a list of tuples, each containing a document and its score.
    """

    # Get the query vector.
    q_vector = dict([
        (a.id, b) for a,b in query_vector(query)
        ])



    # For every document, get its vector,
    # and calculate the cosine similarity between the query vector and the document vector.
    doc_query = Document.select().distinct().join(DocumentTermPairCount).where(DocumentTermPairCount.term << query)

    total_count = doc_query.count()
    cur_count = 0

    page = 1
    got_results = True
    while got_results:
        got_results = False
        document_page = doc_query.paginate(page, 100)
        document_models = dict()
        for doc in document_page:
            document_models[doc.id] = doc
            cur_count += 1
        
        yield cur_count, total_count

        document_vecs = document_vectors(document_page)
        doc_vec_dict = dict()

        for document, term_id, tf_idf in document_vecs.tuples():
            if term_id is None: continue
            if tf_idf is None: continue
            doc_vec_dict[document] = doc_vec_dict.get(document, []) + [(term_id, tf_idf)]

        print(list(doc_vec_dict))

        for document, doc_vec in doc_vec_dict.items():
            if use_cosine:
                document_score = get_cosine_similarity(q_vector, doc_vec)
            else:
                document_score = 0
                for term, tf_idf in doc_vec:
                    if tf_idf is None:
                        continue
                    if term in q_vector:
                        document_score += tf_idf * q_vector[term]
            got_results = True
            yield document_score, document_models[document]
        page += 1