import spacy
import streamlit as st
from spacy_streamlit import visualize_ner

try:
    spacy.load("en_core_web_sm")
except:
    spacy.cli.download("en_core_web_sm")

st.write("""
# Finding tech trends on Data Science jobs using NER. (DEMO)
""")

nlp = spacy.load("en_core_web_sm")
ruler = nlp.add_pipe("entity_ruler", before="ner")
ruler.from_disk("data/patterns.jsonl")

txt = st.text_area(label='Enter the job description')
doc = nlp(txt)
visualize_ner(doc, labels=nlp.get_pipe("ner").labels)

st.json(
    [{'label': entity.label_, 'text': entity.text, 'start': entity.start, 'end': entity.end} \
        for entity in doc.ents if entity.ent_id_ == 'SKILLS']
)
