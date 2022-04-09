import peewee as pw
import logging

logging.basicConfig(level=logging.DEBUG)

#db = pw.SqliteDatabase('database.db')

database_path = '/mnt/Data/database.db'
db = pw.SqliteDatabase(database_path, timeout=15, pragmas={'temp_store_directory': '"/mnt/Data/"', 'journal_mode': 'wal'})

class MyModel(pw.Model):
    class Meta:
        database = db


# Special sentinel value for the `last_updated` field
# to force the update of a value.
INVALIDATE = pw.datetime.datetime.min

# Alias for current datetime
now = pw.datetime.datetime.now

def create_table(model: pw.Model):
    """
    Creates a table for a model.
    Return the same model -- that way it can be used as a decorator.
    """
    db.create_tables([model], safe=True)
    return model

@create_table
class Document(MyModel):
    """Single instance of document in corpus"""
    title = pw.CharField(index=True)
    source = pw.CharField(null=True, index=True)
    content = pw.TextField()
    last_updated = pw.DateTimeField(default=pw.datetime.datetime.now, index=True)

@create_table
class NormalizedTerm(MyModel):
    """Term after normalization and stemming"""
    name = pw.CharField(index=True)

@create_table
class NaturalTerm(MyModel):
    """
    Term as it appears in the document.
    
    This is what the term looked like before stemming and normalization, but lowercased.
    If a word is not here, it is surely not in any document.
    """
    name = pw.CharField(index=True)
    normalized_form = pw.ForeignKeyField(NormalizedTerm, null=True, backref='natural_terms')

@create_table
class DocumentTermPairCount(MyModel):
    """
    Counts the number of times that this term appears in this document.
    
    This caches `Document.content.split().count(term)` for each term and `Document`.

    This is used both to calculate the TF-IDF score of a term in a document,
    and it also forms the inverse index to find documents that contain a term.
    """
    document = pw.ForeignKeyField(Document, on_delete='CASCADE')
    term = pw.ForeignKeyField(NormalizedTerm)
    count = pw.IntegerField()
    last_updated = pw.DateTimeField(default=pw.datetime.datetime.now, index=True)

    class Meta:
        primary_key = pw.CompositeKey('document', 'term')

@create_table
class DocumentsContainingTermCount(MyModel):
    """
    Counts the number of documents that contain this term at least once.
    
    This caches `len([doc for doc in Document if term in doc.content.split()])` for each term.
    """
    term = pw.ForeignKeyField(NormalizedTerm)
    count = pw.IntegerField(index=True)
    last_updated = pw.DateTimeField(default=pw.datetime.datetime.now, index=True)

    class Meta:
        primary_key = pw.CompositeKey('term')


@create_table
class DocumentTotalTermCount(MyModel):
    """
    Counts the total number of non-unique terms in the document.

    This caches `len(doc.content.split())` for each document.
    """
    document = pw.ForeignKeyField(Document, on_delete='CASCADE')
    count = pw.IntegerField(index=True)
    last_updated = pw.DateTimeField(default=pw.datetime.datetime.now, index=True)

    class Meta:
        primary_key = pw.CompositeKey('document')

@create_table
class DocumentTermPosition(MyModel):
    """
    Position of a term in the document.

    Acts like the document's forward index.
    """
    document = pw.ForeignKeyField(Document, on_delete='CASCADE')
    term = pw.ForeignKeyField(NormalizedTerm)
    position = pw.IntegerField()

    class Meta:
        primary_key = pw.CompositeKey('document', 'term', 'position')
