CREATE TABLE "Personne" (
	"id" serial NOT NULL UNIQUE,
	"nom" varchar,
	"prenom" varchar,
	"id_pere" integer,
	"id_mere" integer,
	CONSTRAINT "Personne_pk" PRIMARY KEY ("id")
) WITH (
  OIDS=FALSE
);



CREATE TABLE "Commune" (
	"id" serial NOT NULL,
	"id_departement" integer NOT NULL,
	CONSTRAINT "Commune_pk" PRIMARY KEY ("id")
) WITH (
  OIDS=FALSE
);



CREATE TABLE "Departement" (
	"id" integer NOT NULL,
	CONSTRAINT "Departement_pk" PRIMARY KEY ("id")
) WITH (
  OIDS=FALSE
);



CREATE TABLE "Acte" (
	"id" serial NOT NULL,
	"type" varchar NOT NULL,
	"id_pers_A" integer NOT NULL,
	"id_pers_B" integer NOT NULL,
	"id_commune" integer NOT NULL,
	"num_vue" varchar,
	CONSTRAINT "Acte_pk" PRIMARY KEY ("id")
) WITH (
  OIDS=FALSE
);



CREATE TABLE "Type" (
	"nom" varchar NOT NULL,
	CONSTRAINT "Type_pk" PRIMARY KEY ("nom")
) WITH (
  OIDS=FALSE
);



ALTER TABLE "Personne" ADD CONSTRAINT "Personne_fk0" FOREIGN KEY ("id_pere") REFERENCES "Personne"("id");
ALTER TABLE "Personne" ADD CONSTRAINT "Personne_fk1" FOREIGN KEY ("id_mere") REFERENCES "Personne"("id");

ALTER TABLE "Commune" ADD CONSTRAINT "Commune_fk0" FOREIGN KEY ("id_departement") REFERENCES "Departement"("id");


ALTER TABLE "Acte" ADD CONSTRAINT "Acte_fk0" FOREIGN KEY ("type") REFERENCES "Type"("nom");
ALTER TABLE "Acte" ADD CONSTRAINT "Acte_fk1" FOREIGN KEY ("id_pers_A") REFERENCES "Personne"("id");
ALTER TABLE "Acte" ADD CONSTRAINT "Acte_fk2" FOREIGN KEY ("id_pers_B") REFERENCES "Personne"("id");
ALTER TABLE "Acte" ADD CONSTRAINT "Acte_fk3" FOREIGN KEY ("id_commune") REFERENCES "Commune"("id");







