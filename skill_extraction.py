import nltk
import spacy
import ssl
import concurrent.futures

ssl._create_default_https_context = ssl._create_unverified_context
nltk.download('stopwords')

from nltk.corpus import stopwords
sw = stopwords.words('english')

from utils import Db

nlp = spacy.load("en_core_web_sm")
ruler = nlp.add_pipe("entity_ruler", before="ner")
ruler.from_disk("data/patterns.jsonl")


def extract(row_id):
    text = Db().get_job_description(row_id)
    doc = nlp(text)
    frameworks = []
    databases = []
    platforms = []
    prog_langs = []

    for entity in doc.ents:
        if entity.ent_id_ == 'SKILLS':
            if entity.label_ == 'PROG_LANG' and entity.text not in prog_langs:
                prog_langs.append(entity.text)
            if entity.label_ == 'PLATFORM' and entity.text not in platforms:
                platforms.append(entity.text)
            if entity.label_ == 'DB' and entity.text not in databases:
                databases.append(entity.text)
            if entity.label_ == 'FRAMEWORKS' and entity.text not in frameworks:
                frameworks.append(entity.text)

    frameworks = " ".join(frameworks)
    databases = " ".join(databases)
    platform = " ".join(platform)
    prog_langs = " ".join(prog_langs)
    data = (prog_langs, platform, databases, frameworks, row_id)
    Db().update_skilss(data)


if __name__ == '__main__':
    with concurrent.futures.ThreadPoolExecutor(max_workers=250) as executor:
        worker_to_queue = {
            executor.submit(extract, row_id=x): x for x in Db().waiting_for_extract()
        }
        for worker in concurrent.futures.as_completed(worker_to_queue):
            worker_to_queue[worker]
