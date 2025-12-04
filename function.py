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

def add_players(self):
    csv_files = [
        "campbell_fighting_camels_fixed.csv",
        "clean_florida_gators_2025_roster.csv",
        "duke_blue_devils_roster.csv",
        "lsu_tigers_roster.csv"
        "clean_alabama_crimson_tide_2025_roster.csv"
        "clean_georgia_bulldogs_2025_roster.csv"
        "clean_miami_hurricanes_2025_roster.csv"
        "clean_mississippi_state_bulldogs_2025_roster.csv"
        "clean_north_carolina_tar_heels_2025_roster.csv"
        "clean_ohio_state_buckeyes_2025_roster.csv"
        "clean_pittsburgh_panthers_2025_roster.csv"
        "clean_rice_owls_2025_roster.csv"
        "clean_tennessee_volunteers_2025_roster.csv"
        "clean_texas_longhorns_2025_roster.csv"
        "coastal_carolina_roster_clean.csv"
        "dallas_baptist_2025_roster.csv"
        "louisiana_tech_bulldogs_players.csv"   
        "southern_indiana_players.csv"
        "uc_irvine_anteaters_roster.csv"
        "ucsb_roster.csv"
    ]

    query = """
        LOAD CSV WITH HEADERS FROM $url AS row
        MERGE (p:Player {name: row.Name, schoolName: row.School})
        SET p.position = CASE WHEN row.Position = 'N/A' THEN NULL ELSE row.Position END,
            p.heightInches =
                toInteger(split(row.Height, "'")[0]) * 12 +
                toInteger(replace(split(row.Height, "'")[1], '\"', '')),
            p.weight = toInteger(replace(row.Weight, " lb", "")),
            p.team = row.Team;
    """

    for file in csv_files:
        url = f"{RAW_BASE}/{file}"
        print("Loading:", url)
        self.driver.execute_query(query, url=url, database_=self.DATABASE)

    print("Players added successfully.")

def add_coaches(self):
    url = f"{RAW_BASE}/coaches_extended.csv"

    # Cypher to load head coach + assistants
    query = """
    LOAD CSV WITH HEADERS FROM $url AS row

    // Create School node
    MERGE (s:School {name: row.School})

    // Create Head Coach
    MERGE (hc:Coach {name: row.`Head Coach`})
    MERGE (hc)-[:HEAD_COACH_OF]->(s)

    // Create Assistant Coaches (split by semicolon)
    FOREACH (assistant IN split(row.`Assistant Coaches`, ';') |
        MERGE (ac:Coach {name: trim(assistant)})
        MERGE (ac)-[:ASSISTANT_COACH_OF]->(s)
    );
    """

    self.driver.execute_query(query, url=url, database_=self.DATABASE)
    print("Coaches added successfully.")

def add_teams(self):
    url = f"{RAW_BASE}/teams.csv"

    query = """
        LOAD CSV WITH HEADERS FROM $url AS row
        MERGE (t:Team {name: row.School});
    """

    self.driver.execute_query(query, url=url, database_=self.DATABASE)
    print("Teams added successfully.")

def add_conferences(self):
    url = f"{RAW_BASE}/conferences_enriched.csv"

    query = """
        LOAD CSV WITH HEADERS FROM $url AS row
        MERGE (c:Conference {name: row.Conference})
        SET c.region = row.Region,
            c.abbreviation = row.Abbreviation,
            c.foundedYear = toInteger(row.Founded),
            c.numberOfTeams = toInteger(row.NumberOfTeams),
            c.headquarters = row.Headquarters;
    """

    self.driver.execute_query(query, url=url, database_=self.DATABASE)
    print("Conferences added successfully.")

def add_schools(self):
    url = f"{RAW_BASE}/schools.csv"

    query = """
        LOAD CSV WITH HEADERS FROM $url AS row
        MERGE (s:School {name: row.School});
    """

    self.driver.execute_query(query, url=url, database_=self.DATABASE)
    print("Schools added successfully.")

def add_player_relationships(self):
    url = f"{RAW_BASE}/players.csv"

    query = """
        LOAD CSV WITH HEADERS FROM $url AS row
        
        // Match the player
        MATCH (p:Player {name: row.Name, schoolName: row.School})
        
        // Match the player's team
        MATCH (t:Team {name: row.Team})
        MERGE (p)-[:PLAYS_FOR]->(t)

        // Match the school they attend
        MATCH (s:School {name: row.School})
        MERGE (p)-[:ENROLLED_AT]->(s);
    """

    self.driver.execute_query(query, url=url, database_=self.DATABASE)
    print("Player relationships added successfully.")

def add_team_relationships(self):
    url = f"{RAW_BASE}/conferences_enriched.csv"

    query = """
        LOAD CSV WITH HEADERS FROM $url AS row
        
        // Match Team by School name in conferences.csv
        MATCH (t:Team {name: row.School})
        
        // Match Conference by conference name
        MATCH (c:Conference {name: row.Conference})
        MERGE (t)-[:MEMBER_OF]->(c)

        // Link team to school node
        MATCH (s:School {name: row.School})
        MERGE (t)-[:REPRESENTS]->(s);
    """

    self.driver.execute_query(query, url=url, database_=self.DATABASE)
    print("Team relationships added successfully.")

def add_coach_relationships(self):
    url = f"{RAW_BASE}/coaches.csv"

    query = """
        LOAD CSV WITH HEADERS FROM $url AS row

        // Match the team by its name
        MATCH (t:Team {name: row.Team})

        // --- HEAD COACH ---
        MERGE (hc:Coach {name: row.`Head Coach`})
        SET hc.role = "Head Coach"
        MERGE (hc)-[:COACHES]->(t)

        // --- ASSISTANT COACHES ---
        WITH t, split(row.`Assistant Coaches`, ";") AS assistants
        UNWIND assistants AS a
        WITH t, trim(a) AS assistantName
        MERGE (ac:Coach {name: assistantName})
        SET ac.role = "Assistant Coach"
        MERGE (ac)-[:ASSISTS]->(t);
    """

    self.driver.execute_query(query, url=url, database_=self.DATABASE)
    print("Coach relationships added successfully.")

def delete_all(self):
        query = "MATCH (n) DETACH DELETE n;"
        self.driver.execute_query(query, database_=self.DATABASE)
        print("All nodes and relationships deleted successfully.")

def load_all(self):
        self.delete_all()
        self.create_constraints()
        self.add_conferences()
        self.add_schools()
        self.add_teams()
        self.add_players()
        self.add_coaches()

        self.add_player_relationships()
        self.add_team_relationships()
        self.add_coach_relationships()
        self.close()

if __name__ == "__main__":
    manager = GraphDBManager()
    manager.load_all()
