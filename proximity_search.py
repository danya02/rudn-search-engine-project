from typing import Dict, Generator, Iterable, List, Tuple

from attr import frozen
from database import *
from document_stats import get_natural_term, normalize_term
from snippet_tools import get_snippet, get_snippet_internal
from tf_idf_tools import get_term_idf, get_term_tf
import random

# Weighting parameters for the proximity score
class Weights:
    term_missing_penalty = 10000  # Penalty for each term in the query missing from the document
    base_distance_cost = 1  # Base cost for each distance between terms
    sampling_rate = 0.1  # How much of the combinations to sample
    windowing_word_count = 40 # How many words to consider in the window

def get_placements(document: List[NormalizedTerm], terms: List[NormalizedTerm], current_placements = dict()) -> Generator[Dict[NormalizedTerm, int], None, None]:
    """
    Get all possible placements of the terms in the query.
    """

    if not terms:
        if random.random() < Weights.sampling_rate:
            yield current_placements
        return
    
    # Get the first term
    term = terms[0]

    # Get its positions in the document
    positions = [
        dtp.position for dtp in DocumentTermPosition.select().where(
            DocumentTermPosition.document == document,
            DocumentTermPosition.term == term
        ).order_by(DocumentTermPosition.position)
    ]

    # For each position, try to place the term
    for position in positions:
        new_placements = current_placements.copy()
        new_placements[term] = position

        # Recurse
        yield from get_placements(document, terms[1:], new_placements)




def proximity_score_slow(query: List[NormalizedTerm], document: Document) -> float:
    """
    Get the proximity score for the terms in the query and the given document.
    """
    
    # Get all terms that are present in the document
    unique_document_terms = set([
        doc.term for doc in DocumentTermPairCount.select().where(
            DocumentTermPairCount.document == document
        )
    ])

    # Get terms that are in both the document and the query
    common_terms = set(query).intersection(unique_document_terms)

    # If there are no common terms, or there is only one,
    # the proximity score is meaningless,
    # so return infinity.
    if not common_terms or len(common_terms) == 1:
        return float('inf'), dict()

    # For every missing term, assign a penalty.
    score_offset = Weights.term_missing_penalty * (len(query) - len(common_terms))

    # Get the IDF for each term and use that as a weight for the proximity score
    term_idfs = dict()
    for term in common_terms:
        term_idfs[term] = get_term_idf(term)
    
    # Enumerate the different placement options
    # TODO: make enumeration smarter (Q asked at https://stackoverflow.com/q/71724084/5936187)

    best_placement = None
    best_score = float('inf')

    for placement in get_placements(document, list(common_terms)):
        score = score_offset
        for first_term in placement:
            for second_term in placement:
                if first_term.id < second_term.id:  # Only consider each term pair once.
                    continue

                importance = term_idfs[first_term] * term_idfs[second_term]
                distance = abs(placement[first_term] - placement[second_term])
                score += (distance * Weights.base_distance_cost) / importance
        if score < best_score:
            best_placement = placement
            best_score = score
    
    return best_score, best_placement


def proximity_score_windowed(query: List[NormalizedTerm], document: Document) -> float:
    """
    Measure the proximity score for the given document and query,
    but use window selections for the document terms.
    """

    # Get all the terms in the document as a list
    document_terms = [
        doc.term for doc in DocumentTermPairCount.select().where(
            DocumentTermPairCount.document == document
        )
    ]

    # Get the document's TF and IDF for each term in the query
    term_tfs = dict()
    term_idfs = dict()
    for term in query:
        term_tfs[term] = document_terms.count(term) / len(document_terms)
        term_idfs[term] = get_term_idf(term)

    # Get the positions of the terms from the query in the document
    query_term_positions = [
        i for i, term in enumerate(document_terms) if term in query
    ]

    # For each term position, check the window starting and ending at it
    # If this window contains more than the current captured number of words,
    # update both the best window list and the number of words that the best window must contain.

    best_word_count = 0
    best_window_list = []

    def slice_by_window(list: List[Document], window: range) -> List[Document]:
        return list[window.start:window.stop:window.step]

    for term_position in query_term_positions:
        window_anchor = term_position
        left_window = range(
            max(0, window_anchor - Weights.windowing_word_count),
            window_anchor+1
        )
        right_window = range(
            window_anchor,
            min(len(document_terms), window_anchor + Weights.windowing_word_count + 1)
        )
        left_window_terms_present = [
            term for term in slice_by_window(document_terms, left_window)
            if term in query
        ]
        right_window_terms_present = [
            term for term in slice_by_window(document_terms, right_window)
            if term in query
        ]

        # If the new windows contain more of the query terms than the past best windows,
        # forget all the old windows and start over.
        left_window_unique_terms = set(left_window_terms_present)
        right_window_unique_terms = set(right_window_terms_present)
        if len(left_window_unique_terms) > best_word_count:
            best_word_count = len(left_window_unique_terms)
            best_window_list = [left_window]
        elif len(left_window_unique_terms) == best_word_count:
            best_window_list.append(left_window)
        
        if len(right_window_unique_terms) > best_word_count:
            best_word_count = len(right_window_unique_terms)
            best_window_list = [right_window]
        elif len(right_window_unique_terms) == best_word_count:
            best_window_list.append(right_window)
        

    # If there are no windows, return infinity (the worst score)
    if not best_window_list:
        return float('inf')
        
    # If there is only one window, return the TF-IDF for the query terms that fall inside that window.
    if len(best_window_list) == 1:
        window = best_window_list[0]
        score = 0
        for term in query:
            if term in slice_by_window(document_terms, window):
                score += term_idfs[term] * term_tfs[term]
        return score

    # If there are more than one window, find the window where the total sum of the distances between the terms is minimal.

    # ... but if there is only one term in the window, this score is meaningless,
    # so return infinity.
    if len(best_window_list) == 1:
        return float('inf')
    

    window_scores = [float('inf')]
    print(best_window_list)
    for window in best_window_list:
        window_score = 0
        window_terms = slice_by_window(query, window)
        window_query_term_positions = [i for i, term in enumerate(window_terms) if term in query]
        for first_list_index, first_position in enumerate(window_query_term_positions):
            for second_list_index, second_position in enumerate(window_query_term_positions):
                if first_list_index < second_list_index:
                    window_score += abs(first_position - second_position)
        
        # Now divide the distance by the TF-IDF for each term in the window
        # Low distance, high TF-IDF -> low score -> high sorting
        # High distance or low TF-IDF -> high score -> low sorting
        window_tf_idf = 0
        print(*[x.name if x not in query else f"**{x.name}**" for x in slice_by_window(document_terms, window)])
        print(*[x.name if x not in query else f"**{x.name}**" for x in window_terms])
        for term in filter(lambda x: x in query, window_terms):
            print(term, term_idfs[term], term_tfs[term])
            window_tf_idf += term_idfs[term] * term_tfs[term]
        
        if window_tf_idf == 0:
            continue
        
        window_score /= window_tf_idf
        window_scores.append(window_score)

    # Return the minimum score
    return min(window_scores)


def proximity_based_on_snippets(query: List[NormalizedTerm], document: Document) -> float:
    """
    Calculate the proximity score by computing the snippet for the document and taking its score.
    """
    snippet, score = get_snippet_internal(document, frozenset(query), 100, 50, lambda x: 1)
    return score


def score_all_docs(query: List[NormalizedTerm]) -> Generator[Tuple[float, Document], None, None]:
    """
    Get the proximity scores for all documents.
    """
    total_count = Document.select().count()
    for ind, doc in enumerate(Document.select()):
        yield ind, total_count
        yield proximity_based_on_snippets(query, doc), doc

def score_subset_of_docs(query: List[NormalizedTerm], docs_to_test: Iterable[Document], count: int = None) -> Generator[Tuple[float, Document], None, None]:
    """
    Get the proximity scores for the given set of documents
    """
    total_count = count or Document.select().count()
    for ind, doc in enumerate(docs_to_test):
        yield ind, total_count
        yield proximity_based_on_snippets(query, doc), doc