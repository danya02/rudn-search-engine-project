#from document_stats import index_document, reset_document_counters
#from database import *

#for i in Document.select():
#    reset_document_counters(i)

#for i in Document.select():
#    index_document(i)
import app
app.app.run(debug=True)