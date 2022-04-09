from functools import lru_cache
from cvxopt import normal
from flask import Blueprint, request, abort
import collections
from database import *
import tf_idf_tools
import document_stats

bp = Blueprint('snippets', __name__, url_prefix='/snippets')

@bp.route('/get-snippet')
def get_snippet():
    document_id = request.args['id']
    document = Document.get_or_none(Document.id == document_id)
    if not document:
        return abort(404)
    query = request.args['q']
    if not query:
        return abort(400)
    size = int(request.args.get('size', '50'))

    # Get the terms in the query and find their normal forms.
    terms = query.split()
    natural_terms = [document_stats.get_natural_term(term) for term in terms]
    normal_terms = [document_stats.normalize_term(term) for term in natural_terms]
    normal_terms = frozenset(normal_terms)

    snippet, score = get_snippet_internal(document, normal_terms, size)

    return "<span>..." + snippet + f'...</span><span class="badge bg-primary">Snippet score: {score}</span>'

def get_snippet_internal(document, normal_terms, size, skip=10, loc_weight=lambda x: 1-(x**3)):
    """
    Get a snippet from a document corresponding to the given document ID and query.
    """

    normal_terms = {term.id for term in normal_terms}

    term_idfs = {i: tf_idf_tools.get_term_idf(i) for i in normal_terms}

    document_term_list = [dtp.term for dtp in DocumentTermPosition.select(DocumentTermPosition, NormalizedTerm).join(NormalizedTerm, on=(DocumentTermPosition.term == NormalizedTerm.id)).where(DocumentTermPosition.document == document).order_by(DocumentTermPosition.position)]

    best_window_start = -1
    best_window_end = -1
    best_window_score = -1
    best_interesting_indexes = []

    # Iterate over the windows in the document.
    for window_start in range(0, len(document_term_list) - size + 1, skip):
        window_end = window_start + size
        window = document_term_list[window_start:window_end]
        window_score = 0

        # If the term occurs more than once, such a window is exponentially penalized.
        repeat_penalty = collections.defaultdict(lambda: 1)

        interesting_indexes = []

        # Terms are weighted according to their closeness to the center of the window,
        # as well as their IDF.

        for index, term in enumerate(window):
            if term.id in normal_terms:
                interesting_indexes.append(index)
                position = index / len(window)
                position = (2*position) - 1
                term_presence_score = term_idfs[term.id] * loc_weight(position) * repeat_penalty[term.id]
                window_score += term_presence_score
                repeat_penalty[term.id] /= 2
        
        if window_score > best_window_score:
            best_window_start = window_start
            best_window_end = window_end
            best_window_score = window_score
            best_interesting_indexes = interesting_indexes
    
    document_word_list = document_stats.split_by_good_words(document.content)
    snippet = document_word_list[best_window_start:best_window_end]
    for i in best_interesting_indexes:
        snippet[i] = '<b>' + snippet[i] + '</b>'
    return ' '.join(snippet), best_window_score