import math
from flask import Blueprint, abort, request, render_template, url_for
from database import *

bp = Blueprint('terms', __name__, url_prefix='/terms')

@bp.route('/list')
def list():
    """Show a list of all terms"""
    order = request.args.get('order', 'id')
    terms = DocumentsContainingTermCount.select(DocumentsContainingTermCount, NormalizedTerm).join(NormalizedTerm)

    if order == 'id':
        terms = terms.order_by(NormalizedTerm.id)
    elif order == 'name':
        terms = terms.order_by(NormalizedTerm.name)
    elif order == 'doc-count':
        terms = terms.order_by(DocumentsContainingTermCount.count.desc())

    term_count = terms.count()
    per_page = 1000
    total_pages = math.ceil(term_count / per_page)
    page = int(request.args.get('page', '1'))
    terms = terms.paginate(page, per_page)

    total_documents = Document.select().count()
    def get_idf(item):
        return math.log(total_documents / item.count)

    return render_template('term-list.html', terms=terms, pages=range(1, total_pages+1), total_pages=total_pages, page=page, 
                        link_page=lambda page: url_for('terms.list', order=order, page=page),
                        get_idf=get_idf, order=order)

@bp.route('/natural')
def get_natural():
    id = int(request.args['id'])
    term = NormalizedTerm.get_or_none(NormalizedTerm.id == id)
    if not term: return abort(404)
    return ' '.join([term.name for term in term.natural_terms])

@bp.route('/view/<int:term_id>')
def view(term_id):
    term = NormalizedTerm.select(NormalizedTerm, NaturalTerm).join(NaturalTerm).where(NormalizedTerm.id == term_id).get_or_none()
    if not term: return abort(404)

    documents = Document.select(
        Document, DocumentTermPairCount, DocumentTotalTermCount
        ).join(
            DocumentTermPairCount, attr='dtpc'
        ).switch(Document).join(
            DocumentTotalTermCount, attr='dttc'
        ).where(DocumentTermPairCount.term == term)
    order = request.args.get('order', 'id')
    if order == 'id':
        documents = documents.order_by(Document.id)
    elif order == 'title':
        documents = documents.order_by(Document.title)
    elif order == 'terms':
        documents = documents.order_by(DocumentTermPairCount.count.desc())

    page = int(request.args.get('page', '1'))
    per_page = 100

    doc_count = documents.count()
    total_pages = math.ceil(doc_count / per_page)
    documents = documents.paginate(page, per_page)

    total_documents = Document.select().count()
    idf = math.log(total_documents / doc_count)

    def get_tf(doc):
        return doc.dtpc.count / doc.dttc.count
    
    def get_occurrences(doc):
        return doc.dtpc.count

    return render_template('term-view.html', term=term,
                            documents=documents,
                            pages=range(1, total_pages+1),
                            doc_count = doc_count, idf=idf,
                            natural_terms = '; '.join([i.name for i in term.natural_terms]),
                            link_page=lambda page: url_for('terms.view', term_id=term_id, order=order, page=page),
                            get_tf=get_tf, page=page, get_occurrences=get_occurrences, order=order)