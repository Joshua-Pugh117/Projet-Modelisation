import csv
import psycopg2

# Connect to your postgres DB
conn = psycopg2.connect("dbname=projet_modelisation user=postgres password=motdepasse")

# Open a cursor to perform database operations
cur = conn.cursor()
cmt = 0
# Open the CSV file
with open('mariages\mariages_L3_5k.csv', 'r', encoding='utf-8') as f:
    reader = csv.reader(f)

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

    for row in reader:
        cmt += 1
        if cmt == 10:
            break

#---------------------------insert personneA---------------------------
        # Insert pere
        if row[4] != "n/a": #add check for parent with same name.
            cur.execute(
                """
                INSERT INTO personne (nom_personne, prenom_personne) 
                VALUES (%s, %s)
                """, (row[2],row[4])
            )
        # Insert mere
        if row[5] != "n/a" or row[6] != "n/a": #add check for parent with same name. and fix null verification (only insert if name and surname)
            cur.execute(
                """
                INSERT INTO personne (nom_personne, prenom_personne) 
                VALUES (%s, %s)
                """, (row[5],row[6])
            )
        cur.execute(
            """
            INSERT INTO personne (nom_personne, prenom_personne, id_pere, id_mere) 
            VALUES (%s, %s, (select id_personne from personne where %s = nom_personne and %s = prenom_personne),(select id_personne from personne where %s = nom_personne and %s = prenom_personne))
            """, (row[2], row[3], row[2],row[4], row[5],row[6])
        )
#---------------------------insert personneB---------------------------
        # Insert pere
        if row[9] != "n/a": #add check for parent with same name.
            cur.execute(
                """
                INSERT INTO personne (nom_personne, prenom_personne) 
                VALUES (%s, %s)
                """, (row[7],row[9])
            )
        # Insert mere
        if row[10] != "n/a" or row[11] != "n/a": #add check for parent with same name. and fix null verification (only insert if name and surname)
            cur.execute(
                """
                INSERT INTO personne (nom_personne, prenom_personne) 
                VALUES (%s, %s)
                """, (row[10],row[11])
            )
        cur.execute(
            """
            INSERT INTO personne (nom_personne, prenom_personne, id_pere, id_mere) 
            VALUES (%s, %s, (select id_personne from personne where %s = nom_personne and %s = prenom_personne),(select id_personne from personne where %s = nom_personne and %s = prenom_personne))
            """, (row[7], row[8], row[7],row[9], row[10],row[11])
        )
#---------------------------insert commune---------------------------
        cur.execute(
            """
            INSERT INTO commune (nom_commune, id_departement) 
            SELECT %s, %s
            WHERE NOT EXISTS (SELECT 1 FROM commune WHERE nom_commune = %s AND id_departement = %s);
            """, (row[12], row[13], row[12], row[13])
        )

#---------------------------insert acte---------------------------

        cur.execute(
            """
            INSERT INTO acte (id_acte, type_acte, id_pers_a, id_pers_b, id_commune, num_vue) 
            VALUES (%s,
                    %s, 
                    (select id_personne from personne where %s = nom_personne and %s = prenom_personne limit 1),
                    (select id_personne from personne where %s = nom_personne and %s = prenom_personne limit 1),
                    (SELECT id_commune FROM commune WHERE nom_commune = %s AND id_departement = %s),
                    %s)
            """, (row[0],
                  row[1], 
                  row[2], row[3],
                  row[7], row[8],
                  row[12], row[13],
                  row[15])
        )

# Commit the changes and close the connection
conn.commit()
cur.close()
conn.close()