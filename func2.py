import pandas as pd
from neo4j import GraphDatabase

class DiamondKG:
    def __init__(self, uri, user, password, database="neo4j"):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))
        self.DATABASE = database

    def close(self):
        self.driver.close()

    # --------------------------------------------------
    # 1. CREATE ALL CONSTRAINTS
    # --------------------------------------------------
    def create_constraints(self):
        queries = [
            """
            CREATE CONSTRAINT player_identity_unique IF NOT EXISTS
            FOR (p:Player) REQUIRE (p.name, p.school) IS UNIQUE;
            """,
            """
            CREATE CONSTRAINT coach_identity_unique IF NOT EXISTS
            FOR (c:Coach) REQUIRE (c.name, c.school) IS UNIQUE;
            """,
            """
            CREATE CONSTRAINT team_name_unique IF NOT EXISTS
            FOR (t:Team) REQUIRE t.name IS UNIQUE;
            """,
            """
            CREATE CONSTRAINT school_name_unique IF NOT EXISTS
            FOR (s:School) REQUIRE s.name IS UNIQUE;
            """,
            """
            CREATE CONSTRAINT conference_name_unique IF NOT EXISTS
            FOR (c:Conference) REQUIRE c.name IS UNIQUE;
            """
        ]

        for q in queries:
            self.driver.execute_query(q, database_=self.DATABASE)

        print("✔ Constraints created successfully.")

    # --------------------------------------------------
    # 2. LOAD CONFERENCES
    # --------------------------------------------------
    def load_conferences(self, csv_path):
        df = pd.read_csv(csv_path)

        for _, row in df.iterrows():
            self.driver.execute_query(
                """
                MERGE (c:Conference {name: $name})
                """,
                name=row["Conference"],
                database_=self.DATABASE
            )
        print("✔ Conferences loaded.")

    # --------------------------------------------------
    # 3. LOAD SCHOOLS + RELATION TO CONFERENCE
    # --------------------------------------------------
    def load_schools(self, csv_path):
        df = pd.read_csv(csv_path)

        for _, row in df.iterrows():
            self.driver.execute_query(
                """
                MERGE (s:School {name: $school})
                MERGE (c:Conference {name: $conference})
                MERGE (s)-[:MEMBER_OF]->(c)
                """,
                school=row["School"],
                conference=row["Conference"],
                database_=self.DATABASE
            )
        print("✔ Schools loaded.")

    # --------------------------------------------------
    # 4. LOAD TEAMS
    # --------------------------------------------------
    def load_teams(self, csv_path):
        df = pd.read_csv(csv_path)

        for _, row in df.iterrows():
            self.driver.execute_query(
                """
                MERGE (t:Team {name: $team})
                MERGE (s:School {name: $school})
                MERGE (t)-[:ASSOCIATED_WITH]->(s)
                """,
                team=row["Team"],
                school=row["School"],
                database_=self.DATABASE
            )
        print("✔ Teams loaded.")

    # --------------------------------------------------
    # 5. LOAD PLAYERS + RELATION TO TEAM
    # --------------------------------------------------
    def load_players(self, csv_path):
        df = pd.read_csv(csv_path)

        for _, row in df.iterrows():
            self.driver.execute_query(
                """
                MERGE (p:Player {name: $name, school: $school})
                SET p.position = $position,
                    p.height = $height,
                    p.weight = $weight,
                    p.year = $year
            
                MERGE (t:Team {name: $team})
                MERGE (p)-[:PLAYS_FOR]->(t)
                """,
                name=row["Name"],
                school=row["School"],
                position=row.get("Position"),
                height=row.get("Height"),
                weight=row.get("Weight"),
                year=row.get("Year"),
                team=row["Team"],
                database_=self.DATABASE
            )
        print("✔ Players loaded.")

    # --------------------------------------------------
    # 6. LOAD COACHES (Head + Assistants)
    # --------------------------------------------------
    def load_coaches(self, csv_path):
        df = pd.read_csv(csv_path)

        for _, row in df.iterrows():
            school = row["School"]
            head = row["Head Coach"]
            assistants = str(row["Assistance Coaches"]).split(",")

            # Head Coach
            if pd.notna(head) and head.strip() != "":
                self.driver.execute_query(
                    """
                    MERGE (c:Coach {name: $name, school: $school})
                    MERGE (s:School {name: $school})
                    MERGE (c)-[:COACHES]->(s)
                    """,
                    name=head.strip(),
                    school=school,
                    database_=self.DATABASE
                )

            # Assistant Coaches
            for a in assistants:
                a = a.strip()
                if a != "":
                    self.driver.execute_query(
                        """
                        MERGE (c:Coach {name: $name, school: $school})
                        MERGE (s:School {name: $school})
                        MERGE (c)-[:COACHES]->(s)
                        """,
                        name=a,
                        school=school,
                        database_=self.DATABASE
                    )

        print("✔ Coaches loaded (Head + Assistants).")


# --------------------------------------------------
# RUN EVERYTHING (FIXED)
# --------------------------------------------------
kg = DiamondKG(
    uri="neo4j://127.0.0.1:7687",
    user="neo4j",          # FIXED
    password="Kasuv123@",  # YOUR PASSWORD
    database="neo4j"       # default DB
)

kg.create_constraints()
kg.load_conferences("conferences.csv")
kg.load_schools("schools.csv")
kg.load_teams("teams.csv")
kg.load_players("clean_alabama_crimson_tide_2025_roster.csv")
kg.load_coaches("coaches_extended.csv")
kg.close()
