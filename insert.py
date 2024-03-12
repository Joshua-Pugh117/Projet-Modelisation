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
    reader = list(csv.reader(f)) # Convert to list to get the row count
    row_count = len(reader)

#---------------------------Vide les table--------------------------
    
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

#---------------------------Insert departement---------------------------
    cur.execute(
        "INSERT INTO departement (id_departement) VALUES (%s),(%s),(%s),(%s)", (44,49,79,85)
    )

#---------------------------Insert type_Valide---------------------------
    types_actes = ["Certificat de mariage", 
                   "Contrat de mariage", 
                   "Divorce", 
                   "Mariage",
                   "Promesse de mariage - fiançailles",
                   "Publication de mariage",
                   "Rectification de mariage"]
    
    # Insert type_valide
    for i in types_actes:
        cur.execute(
            "INSERT INTO type_valide (nom_type) VALUES (%s)", (i,)
        )

    # creation d'un dictionnaire pour les lignes qui n'ont pas été insérées
    skiped = {"type": [], "date": []}

    # Recupére chauque ligne (tqdm our afficher la barre de progression)
    for row in tqdm(reader, desc="Loading" , unit="row", total=row_count):
        # si le type d'acte n'est pas dans la liste des types valides, on le met dans le dictionnaire et on passe à la ligne suivante
        if row[1] not in types_actes:
            skiped.get("type").append(row)
            continue

#---------------------------Insert personneA---------------------------
        # Insert pere
        if row[4] != "n/a": 
            cur.execute(
                """
                INSERT INTO personne (nom_personne, prenom_personne) 
                select %s, %s
                WHERE NOT EXISTS (SELECT 1 FROM personne WHERE nom_personne = %s AND prenom_personne = %s);
                """, (row[2],row[4],row[2],row[4])
            )
        # Insert mere
        if row[5] != "n/a" or row[6] != "n/a":
            cur.execute(
                """
                INSERT INTO personne (nom_personne, prenom_personne) 
                select %s, %s
                WHERE NOT EXISTS (SELECT 1 FROM personne WHERE nom_personne = %s AND prenom_personne = %s);
                """, (row[5],row[6],row[5],row[6])
            )
        # Insert personneA
        cur.execute(
            """
            INSERT INTO personne (nom_personne, prenom_personne, id_pere, id_mere) 
            select %s, %s, (select id_personne from personne where %s = nom_personne and %s = prenom_personne),(select id_personne from personne where %s = nom_personne and %s = prenom_personne)
            WHERE NOT EXISTS (SELECT 1 FROM personne WHERE nom_personne = %s AND prenom_personne = %s);
            """, (row[2], row[3], row[2],row[4], row[5],row[6], row[2], row[3])
        )
#---------------------------Insert personneB---------------------------
        # Insert pere
        if row[9] != "n/a":
            cur.execute(
                """
                INSERT INTO personne (nom_personne, prenom_personne) 
                select %s, %s
                WHERE NOT EXISTS (SELECT 1 FROM personne WHERE nom_personne = %s AND prenom_personne = %s);
                """, (row[7],row[9], row[7],row[9])
            )
        # Insert mere
        if row[10] != "n/a" or row[11] != "n/a":
            cur.execute(
                """
                INSERT INTO personne (nom_personne, prenom_personne) 
                select %s, %s
                WHERE NOT EXISTS (SELECT 1 FROM personne WHERE nom_personne = %s AND prenom_personne = %s);
                """, (row[10],row[11],row[10],row[11])
            )
        # Insert personneB
        cur.execute(
            """
            INSERT INTO personne (nom_personne, prenom_personne, id_pere, id_mere) 
            select %s, %s, (select id_personne from personne where %s = nom_personne and %s = prenom_personne),(select id_personne from personne where %s = nom_personne and %s = prenom_personne)
            WHERE NOT EXISTS (SELECT 1 FROM personne WHERE nom_personne = %s AND prenom_personne = %s);
            """, (row[7], row[8], row[7],row[9], row[10],row[11], row[7], row[8])
        )
#---------------------------Insert commune---------------------------
        # si le département n'est pas dans la liste des départements, on le transforme en None
        test = "="
        if row[13] not in ["44", "49", "79", "85"]:
            row[13] = None
            test = "IS"
        # Insert commune
        cur.execute(
            """
            INSERT INTO commune (nom_commune, id_departement) 
            SELECT %s, %s
            WHERE NOT EXISTS (SELECT 1 FROM commune WHERE nom_commune = %s AND id_departement """+test+""" %s);
            """, (row[12], row[13], row[12], row[13]) 
        )


#---------------------------Insert acte---------------------------
        
        if row[14] != "n/a":
            # Verifie si la date est au bon format
            try:
                date_obj = datetime.strptime(row[14], "%d/%m/%Y")
                formatted_date = date_obj.strftime("%Y-%m-%d")
            except ValueError:
                # si la date n'est pas au bon format, on la met dans le dictionnaire
                skiped.get("date").append(row)
                pass
        else:
            formatted_date = None

        try:
            # Insert acte
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
                """, (row[0],        # id_acte
                    row[1],          # type_acte
                    row[2], row[3],  # nom et prenom personneA
                    row[7], row[8],  # nom et prenom personneB
                    row[12], row[13],# nom_commune et id_departement
                    formatted_date,  # date
                    row[15])         # num_vue
            )
        except Exception as e:
            print(f"Error: {e}")
            print(f"Row: {row}")

# Commit the changes and close the connection
conn.commit()
cur.close()
conn.close()

# save skipped to json file
with open('skipped.json', 'w') as f:
    json.dump(skiped, f, indent=2, sort_keys=True, default=str)