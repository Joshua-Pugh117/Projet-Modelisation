CREATE TABLE "personne" (
	"id_personne" serial NOT NULL UNIQUE,
	"nom_personne" varchar,
	"prenom_personne" varchar,
	"id_pere" integer,
	"id_mere" integer,
	CONSTRAINT "personne_pk" PRIMARY KEY ("id_personne")
) WITH (
  OIDS=FALSE
);



CREATE TABLE "commune" (
	"id_commune" serial NOT NULL,
	"nom_commune" varchar NOT NULL,
	"id_departement" integer NOT NULL,
	CONSTRAINT "commune_pk" PRIMARY KEY ("id_commune")
) WITH (
  OIDS=FALSE
);



CREATE TABLE "departement" (
	"id_departement" integer NOT NULL,
	CONSTRAINT "departement_pk" PRIMARY KEY ("id_departement")
) WITH (
  OIDS=FALSE
);



CREATE TABLE "acte" (
	"id_acte" serial NOT NULL,
	"type_acte" varchar NOT NULL,
	"id_pers_a" integer NOT NULL,
	"id_pers_b" integer NOT NULL,
	"id_commune" integer NOT NULL,
	"num_vue" varchar,
	CONSTRAINT "acte_pk" PRIMARY KEY ("id_acte")
) WITH (
  OIDS=FALSE
);



CREATE TABLE "type_valide" (
	"nom_type" varchar NOT NULL,
	CONSTRAINT "type_valide_pk" PRIMARY KEY ("nom_type")
) WITH (
  OIDS=FALSE
);



ALTER TABLE "personne" ADD CONSTRAINT "personne_fk0" FOREIGN KEY ("id_pere") REFERENCES "personne"("id_personne");
ALTER TABLE "personne" ADD CONSTRAINT "personne_fk1" FOREIGN KEY ("id_mere") REFERENCES "personne"("id_personne");

ALTER TABLE "commune" ADD CONSTRAINT "commune_fk0" FOREIGN KEY ("id_departement") REFERENCES "departement"("id_departement");


ALTER TABLE "acte" ADD CONSTRAINT "acte_fk0" FOREIGN KEY ("type_acte") REFERENCES "type_valide"("nom_type");
ALTER TABLE "acte" ADD CONSTRAINT "acte_fk1" FOREIGN KEY ("id_pers_a") REFERENCES "personne"("id_personne");
ALTER TABLE "acte" ADD CONSTRAINT "acte_fk2" FOREIGN KEY ("id_pers_a") REFERENCES "personne"("id_personne");
ALTER TABLE "acte" ADD CONSTRAINT "acte_fk3" FOREIGN KEY ("id_commune") REFERENCES "commune"("id_commune");







