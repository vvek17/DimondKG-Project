from neo4j import GraphDatabase
import dotenv
import os

RAW_BASE = "https://github.com/vvek17/DimondKG-Project.git"
# Replace with your actual credentials
URI = "neo4j://127.0.0.1:7687"  
AUTH = ("neo4j", "Kasuv123@")

# Create driver object and test connectivity
with GraphDatabase.driver(URI, auth=AUTH) as driver:
    driver.verify_connectivity()
    print("Connected successfully!")

class GraphDBManager:
    def __init__(self):
        load_status = dotenv.load_dotenv('Neo4j-b9043243-Created-2025-11-16.txt')
        if load_status is False:
            raise RuntimeError("Environment variables not loaded.")
        
        URI = os.getenv("NEO4J_URI")
        AUTH = (os.getenv("NEO4J_USERNAME"), os.getenv("NEO4J_PASSWORD"))

        self.DATABASE = os.getenv("NEO4J_DATABASE")
        self.driver = GraphDatabase.driver(URI, auth=AUTH)
        self.driver.verify_connectivity()
        print("Connected to Neo4j database successfully.")

    def close(self):
        self.driver.close()

def create_constraints(self):
        queries = [
            """CREATE CONSTRAINT player_identity_unique IF NOT EXISTS
                FOR (p:Player) REQUIRE (p.Name,p.School) IS UNIQUE;""",
            """CREATE CONSTRAINT coach_identity_unique IF NOT EXISTS
                FOR (c:Coach) REQUIRE (c.Name, c.School) IS UNIQUE;""",
            """CREATE CONSTRAINT team_name_unique IF NOT EXISTS
                FOR (t:Team) REQUIRE t.Name IS UNIQUE;""",
            """CREATE CONSTRAINT School_name_unique IF NOT EXISTS
                FOR (s:School) REQUIRE s.Name IS UNIQUE;""",
            """CREATE CONSTRAINT conference_name_unique IF NOT EXISTS   
                FOR (c:Conference) REQUIRE c.Name IS UNIQUE;"""
        ]

        for q in queries:
            self.driver.execute_query(q, database_=self.DATABASE)
        print("Constraints created successfully.")

