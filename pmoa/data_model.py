from sqlalchemy import create_engine, Column, Integer, String, BigInteger, JSON, ARRAY
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

from pmoa.article_has_citation import extract_citation_context

# Create an engine to connect to the database


username = 'admin'
password = 'admin'
host = '10.224.68.29'
port = '5432'
database = 'pubmed'

connection_string = f'postgresql://{username}:{password}@{host}:{port}/{database}'

engine = create_engine(connection_string, echo=True)

# Create a session factory
Session = sessionmaker(bind=engine)

# Create a base class for declarative models
Base = declarative_base()


# Define your table model
class CitationContext(Base):
    __tablename__ = 'citation_context'

    pmid = Column(String, primary_key=True)
    context_data = Column(JSON)
    citations_list = Column(ARRAY(String))


# Create the table
Base.metadata.create_all(engine)

# Example usage
# Create a session
session = Session()

# Create a new entry
input_dict = extract_citation_context(path='oa_comm/PMC7706999.xml')
pmid = input_dict['pmid']
context = input_dict['context']
citations = input_dict['citations']


citation_entry = CitationContext(pmid=pmid, context_data=context, citations_list=citations)

# Add the entry to the session
session.add(citation_entry)

# Commit the changes to the database
session.commit()

# Query the citation context table
entries = session.query(citation_entry).all()

# Print the entries
for citation in entries:
    print(citation)

# Close the session
session.close()
