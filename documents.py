import math
from flask import Blueprint, request, render_template, redirect, url_for, abort
from sympy import re
from database import *
import document_stats
import string

bp = Blueprint('documents', __name__, url_prefix='/documents')

@bp.route('/view/<int:document_id>')
def view(document_id):
    """Show the contents of this document"""
    document = Document.get_or_none(Document.id == document_id)
    if not document:
        return abort(404)
    
    norm_highlight = False
    if 'q' in request.args:
        query = request.args['q']
        norm_highlight = request.args.get('norm-highlight', '0') == '1'
        query_normal_terms = [document_stats.normalize_term(document_stats.get_natural_term(term)) for term in query.split()]
        document_terms = [dtp.term for dtp in DocumentTermPosition.select(DocumentTermPosition, NormalizedTerm).where(DocumentTermPosition.document == document).join(NormalizedTerm).order_by(DocumentTermPosition.position)]
        document_words = document_stats.split_by_good_words(document.content)

        c = 0
        for t, w in zip(document_terms, document_words):
            print(t.name, w)
            c += 1
            if c>100: break

        # If a word in the document is not a term, it gets merged with the previous word.
        # This is done to avoid having a word that is not a term in the middle of a document.

        for index, doc_term in enumerate(document_terms):
            if doc_term in query_normal_terms:
                qnt = query_normal_terms.index(doc_term)
                q_t = query.split()[qnt]
                try:
                    document_words[index] = '<b>' + document_words[index] + (f'[<a class="text-success" href="{ url_for("terms.view", term_id=doc_term.id) }">{doc_term.name}</a>]</b>' if norm_highlight else '</b>')
                except: pass
        document.content = ' '.join(document_words)

    return render_template('document_view.html', document=document, query=request.args.get('q', ''), norm_highlight=norm_highlight)

@bp.route('/list')
def list():
    """Show a list of all documents"""
    order = request.args.get('order', 'id')
    documents = Document.select()

    if order == 'id':
        documents = documents.order_by(Document.id)
    elif order == 'title':
        documents = documents.order_by(Document.title)
    elif order == 'terms':
        documents = documents.join(DocumentTotalTermCount).order_by(DocumentTotalTermCount.count.desc())

    document_count = documents.count()
    per_page = 1000
    total_pages = math.ceil(document_count / per_page)
    page = int(request.args.get('page', '1'))
    documents = documents.paginate(page, per_page)

    return render_template('document-list.html', documents=documents,
                    pages=range(1, total_pages+1),
                    page=page,
                    link_page=lambda page: url_for('documents.list', page=page, order=order), order=order)