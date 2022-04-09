from database import *
import document_stats
import os
import bs4
import sqlite3

def import_single_document(path: str):
    """
    Import a single document
    represented as a file.

    Create a new document record,
    with the file's name as the title
    and the file's contents as the content.

    After the record is created, run
    the indexing algorithm on the document.
    """

    # Remove the extension from the file name
    title = os.path.basename(path).split('.')[:-1]
    title = '.'.join(title)

    with open(path, 'r') as f:
        content = f.read()

    with db.atomic():
        document = Document.create(title=title, content=content)
        document_stats.index_document(document)

    return document

def import_directory(path: str):
    """
    Import all the documents in a directory.
    """

    for filename in os.listdir(path):
        import_single_document(os.path.join(path, filename))

def import_html(path: str):
    """
    Import a single HTML file, parsing the <title> tag as the document's title,
    and the textual contents of the <body> tag as the document's content.
    """

    with open(path, 'r') as f:
        soup = bs4.BeautifulSoup(f, 'html.parser')

    title = soup.title.string
    content = '\n'.join(soup.stripped_strings)

    with db.atomic():
        document = Document.create(title=title, content=content)
        document_stats.index_document(document)

    return document

def import_directory_html(path: str):
    """
    Import all the HTML files in a directory.
    """

    for filename in os.listdir(path):
        print(filename)
        import_html(os.path.join(path, filename))

def import_py_ao3_archive(path: str):
    """
    Import documents from the AO3 archive database.
    """

    ao3_db = sqlite3.connect(path)
    cursor = ao3_db.cursor()

    cursor.execute('''
        SELECT work.id, work.title, chapter.chapter_id, chapter."index", chapter.title, chapter.content
        FROM work JOIN chapter ON work.id = chapter.work_id
        ORDER BY length(chapter.content) desc
    ''')

    for work_id, work_title, chapter_id, chapter_index, chapter_title, chapter_content in cursor.fetchall():
        print(work_id, work_title, chapter_id, chapter_index, chapter_title)
        if chapter_id:
            source_url = f'https://archiveofourown.org/works/{work_id}/chapters/{chapter_id}'
        else:
            source_url= f'https://archiveofourown.org/works/{work_id}'
        
        if Document.get_or_none(Document.source == source_url):
            print("Already imported")
            continue

        common_start = '<h3 class="landmark heading" id="work">Chapter Text</h3>'
        if chapter_content.startswith(common_start):
            chapter_content = chapter_content[len(common_start):]
        
        content_soup = bs4.BeautifulSoup(chapter_content, 'html.parser')
        cleaned_content = '\n'.join(content_soup.stripped_strings)

        if chapter_id:
            doc = Document.create(
                title=f"{work_title} -- Chapter {chapter_index}: {chapter_title}",
                content=cleaned_content,
                source=f'https://archiveofourown.org/works/{work_id}/chapters/{chapter_id}'
            )
        else:
            doc = Document.create(
                title=work_title,
                content=cleaned_content,
                source=f'https://archiveofourown.org/works/{work_id}'
            )
        document_stats.index_document(doc)

if __name__ == '__main__':
    import_py_ao3_archive('/mnt/Data/ao3.db')
