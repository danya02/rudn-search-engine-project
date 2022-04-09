import os
from database import *
import traceback

WAL_DIR = '/mnt/Data/document_term_position_wals/'

def read_dtp_wals():
    """
    Import DocumentTermPosition rows into the database from the WAL files, removing them as we go.
    Because we are removing them, be careful using this in a backup-restoration scenario.
    """
    count = len(os.listdir(WAL_DIR))
    for index, file in enumerate(os.listdir(WAL_DIR)):
        print("Importing", file, index, '/', count, (index/count)*100, '%')
        with open(WAL_DIR + file, 'r') as f:
            content = f.read()
            lines = content.split('\n')
            lines = list(filter(None, lines))
            last_line = lines[-1].strip()
            # If trailer is written, then the file is complete, continue importing.
            if last_line == 'END':
                import_dtp_lines(lines[:-1])
        os.remove(WAL_DIR + file)

@db.atomic()
def import_dtp_lines(lines):
    """
    Import a list of DocumentTermPosition rows into the database.
    """
    models = [
        DocumentTermPosition(
            document_id=int(d),
            term_id=int(t),
            position=int(p)
        )
        for d,t,p in [line.split() for line in lines]
    ]
    try:
        DocumentTermPosition.bulk_create(models)
    except pw.IntegrityError:
        for model in models:
            try:
                model.save(force_insert=True)
            except pw.IntegrityError:
                traceback.print_exc()
                print(model, model.document_id, model.term_id, model.position)
                input('Enter to continue, CTRL-C to exit')

@db.atomic()
def import_single_dtp_file(file):
    """
    Import a single DocumentTermPosition file into the database.
    """
    with open(file, 'r') as f:
        content = f.read()
        lines = content.split('\n')
        lines = list(filter(None, lines))
        last_line = lines[-1].strip()
        lines = lines[14590000:]
        # If trailer is written, then the file is complete, continue importing.
        if last_line:
            line_count = 0
            for chunk in pw.chunked(lines, 1000):
                print(line_count, '/', len(lines), 100*line_count/len(lines), '%')
                if line_count % 100000 == 0:
                    print("Checkpointing...")
                    db.commit()

                models = []
                for d,t,p in [line.split() for line in chunk]:
                    models.append(
                        DocumentTermPosition(
                            document_id=int(d),
                            term_id=int(t),
                            position=int(p)
                        )
                    )
                line_count += len(models)
                try:
                    DocumentTermPosition.bulk_create(models)
                except pw.IntegrityError:
                    print("Skip")
#                    for model in models:
#                        try:
#                            model.save(force_insert=True)
#                            print(model, 'saved')
#                        except pw.IntegrityError:
#                            traceback.print_exc()
#                            print(model, model.document_id, model.term_id, model.position)
#                            
#                            #input('Enter to continue, CTRL-C to exit')


if __name__ == '__main__':
    import_single_dtp_file('/tmp/document_term_position_old.txt')
    #read_dtp_wals()
