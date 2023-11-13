from sqlalchemy import create_engine, Column, Integer, String, BigInteger
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

# Create an engine to connect to the database


username = 'admin'
password = 'admin'
host = '127.0.0.1'
port = '5432'
database = 'citation_aggregated_model'

connection_string = f'postgresql://{username}:{password}@{host}:{port}/{database}'

engine = create_engine(connection_string, echo=True)

# Create a session factory
Session = sessionmaker(bind=engine)

# Create a base class for declarative models
Base = declarative_base()


# Define your table model
class CitationAbstract(Base):
    __tablename__ = 'citation_abstract'

    corpus_id = Column(Integer, primary_key=True)
    abstract = Column(String)


class CitationAuthor(Base):
    __tablename__ = 'citation_author'
    corpus_id = Column(Integer, primary_key=True)
    author_id = Column(BigInteger)
    name = Column(String, primary_key=True)


class CitationPapersMeta(Base):
    __tablename__ = 'citation_papers_meta'

    corpus_id = Column(Integer, primary_key=True)
    title = Column(String)
    venue = Column(String)
    year = Column(Integer)
    reference_count = Column(Integer)
    citation_count = Column(Integer)


# Create the table
Base.metadata.create_all(engine)

# Example usage
# Create a session
session = Session()

# Create a new entry
citation_entry = CitationAuthor(corpus_id=12345, author_id=1, name='Shubham')

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
