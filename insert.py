import csv
import psycopg2
from datetime import datetime
import json
from tqdm import tqdm

# Connect to your postgres DB
conn = psycopg2.connect("dbname=projet_modelisation user=anyafontenoy password=' '")

# Open a cursor to perform database operations
cur = conn.cursor()

# Open the CSV file
with open('mariages/mariages_L3_5K.csv', 'r', encoding='utf-8') as f: # Petit fichier propre
#with open('mariages/mariages_L3.csv', 'r', encoding='utf-8') as f: # Grand fichier non propre

    

# with open('mariages\mariages_L3_5k.csv', 'r', encoding='utf-8') as f:
    reader = list(csv.reader(f))
    row_count = len(reader)

#---------------------------empty all---------------------------
    
    cur.execute(
        "delete from acte;"
    )
    cur.execute(
        "delete from personne; ALTER SEQUENCE personne_id_personne_seq RESTART WITH 1"
    )
    cur.execute(
        "delete from commune;"
    )
    cur.execute(
        "delete from departement; ALTER SEQUENCE commune_id_commune_seq RESTART WITH 1"
    )
    cur.execute(
        "delete from type_valide;"
    )

#---------------------------insert departement---------------------------
    cur.execute(
        "INSERT INTO departement (id_departement) VALUES (%s),(%s),(%s),(%s)", (44,49,79,85)
    )

#---------------------------insert type_Valide---------------------------
    types_actes = ["Certificat de mariage", 
                   "Contrat de mariage", 
                   "Divorce", 
                   "Mariage",
                   "Promesse de mariage - fiançailles",
                   "Publication de mariage",
                   "Rectification de mariage"]
    
    for i in types_actes:
        cur.execute(
            "INSERT INTO type_valide (nom_type) VALUES (%s)", (i,)
        )

    # for row in reader:
    skiped = {"type": [], "date": []}

    # Pour afficher la barre de progression 
    for row in tqdm(reader, desc="Loading" , unit="row", total=row_count):
        if row[1] not in types_actes:
            skiped.get("type").append(row)
            continue

#---------------------------insert personneA---------------------------
        # Insert pere
        if row[4] != "n/a": #add check for parent with same name.
            cur.execute(
                """
                INSERT INTO personne (nom_personne, prenom_personne) 
                select %s, %s
                WHERE NOT EXISTS (SELECT 1 FROM personne WHERE nom_personne = %s AND prenom_personne = %s);
                """, (row[2],row[4],row[2],row[4])
            )
        # Insert mere
        if row[5] != "n/a" or row[6] != "n/a": #add check for parent with same name. and fix null verification (only insert if name and surname)
            cur.execute(
                """
                INSERT INTO personne (nom_personne, prenom_personne) 
                select %s, %s
                WHERE NOT EXISTS (SELECT 1 FROM personne WHERE nom_personne = %s AND prenom_personne = %s);
                """, (row[5],row[6],row[5],row[6])
            )
        cur.execute(
            """
            INSERT INTO personne (nom_personne, prenom_personne, id_pere, id_mere) 
            select %s, %s, (select id_personne from personne where %s = nom_personne and %s = prenom_personne),(select id_personne from personne where %s = nom_personne and %s = prenom_personne)
            WHERE NOT EXISTS (SELECT 1 FROM personne WHERE nom_personne = %s AND prenom_personne = %s);
            """, (row[2], row[3], row[2],row[4], row[5],row[6], row[2], row[3])
        )
#---------------------------insert personneB---------------------------
        # Insert pere
        if row[9] != "n/a": #add check for parent with same name.
            cur.execute(
                """
                INSERT INTO personne (nom_personne, prenom_personne) 
                select %s, %s
                WHERE NOT EXISTS (SELECT 1 FROM personne WHERE nom_personne = %s AND prenom_personne = %s);
                """, (row[7],row[9], row[7],row[9])
            )
        # Insert mere
        if row[10] != "n/a" or row[11] != "n/a": #add check for parent with same name. and fix null verification (only insert if name and surname)
            cur.execute(
                """
                INSERT INTO personne (nom_personne, prenom_personne) 
                select %s, %s
                WHERE NOT EXISTS (SELECT 1 FROM personne WHERE nom_personne = %s AND prenom_personne = %s);
                """, (row[10],row[11],row[10],row[11])
            )
        cur.execute(
            """
            INSERT INTO personne (nom_personne, prenom_personne, id_pere, id_mere) 
            select %s, %s, (select id_personne from personne where %s = nom_personne and %s = prenom_personne),(select id_personne from personne where %s = nom_personne and %s = prenom_personne)
            WHERE NOT EXISTS (SELECT 1 FROM personne WHERE nom_personne = %s AND prenom_personne = %s);
            """, (row[7], row[8], row[7],row[9], row[10],row[11], row[7], row[8])
        )
#---------------------------insert commune---------------------------
        test = "="
        if row[13] == "n/a" or row[13] not in ["44", "49", "79", "85"]:
            row[13] = None
            test = "IS"
        cur.execute(
            """
            INSERT INTO commune (nom_commune, id_departement) 
            SELECT %s, %s
            WHERE NOT EXISTS (SELECT 1 FROM commune WHERE nom_commune = %s AND id_departement """+test+""" %s);
            """, (row[12], row[13], row[12], row[13])
        )


#---------------------------insert acte---------------------------
        
        if row[14] != "n/a":
            # Check if date_str is valid format
            try:
                date_obj = datetime.strptime(row[14], "%d/%m/%Y")
                formatted_date = date_obj.strftime("%Y-%m-%d")
            except ValueError:
                skiped.get("date").append(row)
                pass
        else:
            formatted_date = None

        try:
            cur.execute(
                """
                INSERT INTO acte (id_acte, type_acte, id_pers_a, id_pers_b, id_commune, date, num_vue) 
                VALUES (%s,
                        %s, 
                        (select id_personne from personne where %s = nom_personne and %s = prenom_personne),
                        (select id_personne from personne where %s = nom_personne and %s = prenom_personne),
                        (SELECT id_commune FROM commune WHERE nom_commune = %s AND id_departement """+ test +""" %s),
                        %s,
                        %s)
                """, (row[0],
                    row[1], 
                    row[2], row[3],
                    row[7], row[8],
                    row[12], row[13],
                    formatted_date,
                    row[15])
            )
        except Exception as e:
            print(f"Error: {e}")
            print(f"Row: {row}")

# Commit the changes and close the connection
conn.commit()
cur.close()
conn.close()

# save skipped to json file
# with open('skipped.json', 'w') as f:
#     json.dump(skiped, f, indent=2, sort_keys=True, default=str)