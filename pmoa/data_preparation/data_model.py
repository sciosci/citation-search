from sqlalchemy import create_engine, Column, Integer, String, JSON, ARRAY, Boolean
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

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


class RelevanceStore(Base):
    __tablename__ = 'relevance_store'
    id = Column(Integer, primary_key=True, autoincrement=True)
    pmid = Column(String)
    sentence = Column(String)
    cited = Column(String)
    is_relevant = Column(Boolean)


class RelevanceStoreNew(Base):
    __tablename__ = 'relevance_store_new'
    id = Column(Integer, primary_key=True, autoincrement=True)
    pmid = Column(String)
    sentence = Column(String)
    cited_id = Column(String)
    relevance_score = Column(Integer)


class MetaData(Base):
    __tablename__ = 'metadata'
    pmid = Column(String, primary_key=True)
    title = Column(String)
    # abstract = Column(String)
    publication_year = Column(Integer)
    cited_by_count = Column(Integer)


# Create the table
Base.metadata.create_all(engine)
