# Diamond Knowledge Graph - Neo4j Baseball Database

A Neo4j knowledge graph that models college baseball data from 20+ Division I universities. Built with Python web scraping and Cypher queries to organize players, coaches, teams, and conferences.

## What This Project Does

This project takes roster data from college baseball websites and organizes it into a graph database. You can ask questions like "Which players transferred teams?" or "Show me all players from a specific conference" and get instant answers.

**What's included:**
- 20+ universities (LSU, UNC, Texas, Miami, Duke, Tennessee, Georgia, Alabama, Mississippi State, etc.)
- 800+ player records (with position, height, weight, class year, hometown)
- 40+ coaches linked to their teams
- 10 athletic conferences (SEC, ACC, Big 12, and others)

## Technologies Used

- **Neo4j** - Graph database
- **Cypher** - Query language for Neo4j
- **Python** - Web scraping and data processing
- **BeautifulSoup & Selenium** - Extracting data from websites
- **Pandas** - Organizing and cleaning data

## How to Use

### 1. Install Requirements
```bash
pip install beautifulsoup4 selenium pandas requests neo4j
```

### 2. Set up Neo4j
- Create a free account at https://neo4j.com/cloud/platform/aura-db/
- Get your connection details
- Update your Python scripts with the credentials

### 3. Import the Data
Use Cypher commands to load the CSV files into Neo4j:

```cypher
LOAD CSV WITH HEADERS FROM 'file:///schools.csv' AS row
MERGE (s:School {name: row.schoolName});

LOAD CSV WITH HEADERS FROM 'file:///clean_alabama_crimson_tide_2025_roster.csv' AS row
MERGE (p:Player {name: row.Name, team: row.Team, position: row.Position});
```

## Example Queries

**List all teams:**
```cypher
MATCH (t:Team)
RETURN t.teamName
ORDER BY t.teamName
```

**Find tall players (6'9"):**
```cypher
MATCH (p:Player)
WHERE p.heightInches = 81
RETURN p.name, p.team, p.position
```

**See how many teams are in each conference:**
```cypher
MATCH (c:Conference)<-[:MEMBER_OF]-(t:Team)
RETURN c.confName, count(t) AS number_of_teams
ORDER BY number_of_teams DESC
```

**Find players who switched teams:**
```cypher
MATCH (p:Player)-[:PLAYS_FOR {year:2024}]->(t1:Team),
      (p)-[:PLAYS_FOR {year:2025}]->(t2:Team)
WHERE t1 <> t2
RETURN p.name, t1.teamName AS from_team, t2.teamName AS to_team
```

## Challenges I Solved

1. **Inconsistent data formats** - Heights were stored as "6'5"" or "6-5". I converted everything to inches.

2. **Duplicate entries** - Some schools had team names in different cases. I standardized everything before adding to the database.

3. **Different website layouts** - Each school's website had a different structure. I wrote custom scrapers for the hardest ones using Selenium.

4. **Missing relationships** - Rosters didn't include conference info. I manually created a mapping file and linked everything properly.

## Files in This Repo

- `function.py` and `func2.py` - Python scripts for scraping websites
- `schools.csv`, `teams.csv`, `conferences.csv` - Main data files
- `coaches_extended.csv` - Coach information
- `clean_*.csv` - Cleaned player rosters (20+ files)

## Database Stats

- **589+ nodes** (Players, Teams, Coaches, Schools, Conferences)
- **575+ relationships** (PLAYS_FOR, COACHES, MEMBER_OF)
- **800+ players** across 20+ universities
- **40+ coaches**

## What I Learned

- How to design graph databases from scratch
- How to scrape data from different website styles
- How to write complex queries in Cypher
- How to validate and test a large database

## Future Ideas

- Add stats (batting average, ERA, etc.)
- Track recruiting pipelines
- Create an interactive dashboard
- Add historical data from past seasons

## Author

Vivek Solanki  
viveksolanki.17122002@gmail.com  
[GitHub](https://github.com/vvek17)

## License

MIT License - Use this freely for anything you want
