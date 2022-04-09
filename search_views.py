import time
from xml.dom.minidom import Element
from flask import Blueprint, Response, render_template, redirect, render_template_string, stream_with_context, url_for, abort, request
import traceback
from database import *
import search

bp = Blueprint('search', __name__, url_prefix='/search')

@bp.route('/')
def index():
    return search_stream()

@bp.route('/all-at-once')
def all_at_once():
    if 'q' in request.args:
        query = request.args['q']
        algo = request.args.get('algo', 'tf-idf')
        page = int(request.args.get('page', '1'))
        query_results = list(search.run_search(query, algo))
        print(query_results)
        # TF-IDF: higher score is better
        # Proximity: lower score is better.
        query_results.sort(key=lambda x: x[0], reverse=(algo == 'tf-idf'))
        return render_template('search.html', query=query, page=page, algo=algo, results=query_results)
    else:
        return render_template('search.html')

@bp.route('/stream')
def search_stream():
    if 'q' in request.args:
        query = request.args['q']
        algo = request.args.get('algo', 'tf-idf')
        page = int(request.args.get('page', '1'))
        search.run_search(query, algo)
        
        def streaming():
            start_time = time.perf_counter()
            try:
                yield render_template("search-streaming-header.html", query=query, algo=algo)
                rows = 0
                for item in search.run_search(query, algo):
                    print(repr(item))
                    if isinstance(item, tuple) and len(item)==2 and isinstance(item[0], float) and isinstance(item[1], Document):
                        score, doc = item
                        score = score*1000000
                        if score == 0: continue
                        #if algo=='proximity':
                        #    score = -score
                        rows += 1
                        #if rows%100 == 0:
                        #    yield render_template("search-streaming-do_sort.html")
                        yield render_template("search-streaming-result.html", score=score, result=doc, query=query, algo=algo, row=rows)
                    
                    elif isinstance(item, tuple) and len(item)==2 and isinstance(item[0], int) and isinstance(item[1], int):
                        yield render_template("search-streaming-set-element.html",
                            element="doc-counter", content=f"(processed {item[0]} out of {item[1]} documents)",
                            plaintext=f"Processed {item[0]} out of {item[1]} documents so far...")

                yield f"<h2>Time spent: {time.perf_counter() - start_time}</h2>"
                yield render_template("search-streaming-footer.html", query=query, algo=algo, rows=rows)
            except:
                trace = traceback.format_exc()
                yield '<h1 class="text-danger">Error while streaming! Reload page to try again.</h1>'
                yield "<pre>" + trace + "</pre>"
                traceback.print_exc()
        return Response(stream_with_context(streaming()), mimetype='text/html')
