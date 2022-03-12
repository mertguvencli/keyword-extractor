from numpy import place
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


with open('data/sample_description.txt', 'r') as f:
    description = f.read()

text = st.text_area(label='Job Description', value=description, placeholder='Please enter a job description')

doc = nlp(text)
visualize_ner(doc, labels=nlp.get_pipe("ner").labels)

st.subheader('Skill Requirements')
st.json(
    [{'label': entity.label_, 'text': entity.text, 'start': entity.start, 'end': entity.end} \
        for entity in doc.ents if entity.ent_id_ == 'SKILLS']
)
