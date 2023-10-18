CREATE TABLE "crunchbase" (
  "id" integer,
  "uuid" uuid PRIMARY KEY,
  "domain" varchar,
  "name" varchar,
  "permalink" varchar,
  "crunchbase_created_at" timestamp,
  "crunchbase_updated_at" timestamp,
  "country" varchar,
  "status" varchar,
  "founded_on" date,
  "employee_count" varchar,
  "created_at" timestamp,
  "updated_at" timestamp
);

CREATE TABLE "crunchbase_contact" (
  "uuid" uuid PRIMARY KEY,
  "email" varchar,
  "phone" varchar,
  "facebook_url" varchar,
  "linkedin_url" varchar,
  "twitter_url" varchar,
  "region" varchar,
  "city" varchar,
  "address" varchar,
  "postal_code" varchar,
  "created_at" timestamp,
  "updated_at" timestamp
);

CREATE TABLE "crunchbase_description" (
  "uuid" uuid PRIMARY KEY,
  "short_description" varchar NOT NULL,
  "created_at" timestamp,
  "updated_at" timestamp
);

CREATE TABLE "crunchbase_categories" (
  "uuid" uuid,
  "category" varchar,
  "created_at" timestamp,
  "updated_at" timestamp,
  PRIMARY KEY ("uuid", "category")
);

CREATE TABLE "crunchbase_category_groups" (
  "uuid" uuid,
  "category_group" varchar,
  "created_at" timestamp,
  "updated_at" timestamp,
  PRIMARY KEY ("uuid", "category_group")
);

CREATE TABLE "crunchbase_aliases" (
  "uuid" uuid,
  "alias" varchar,
  "created_at" timestamp,
  "updated_at" timestamp,
  PRIMARY KEY ("uuid", "alias")
);

CREATE TABLE "crunchbase_roles" (
  "uuid" uuid,
  "role" varchar,
  "created_at" timestamp,
  "updated_at" timestamp,
  PRIMARY KEY ("uuid", "role")
);

CREATE TABLE "crunchbase_funding" (
  "uuid" uuid PRIMARY KEY,
  "num_funding_rounds" integer,
  "total_funding_usd" integer,
  "total_funding" integer,
  "total_funding_currency_code" varchar,
  "last_funding_on" date,
  "closed_on" date,
  "created_at" timestamp,
  "updated_at" timestamp
);

ALTER TABLE "crunchbase_contact" ADD FOREIGN KEY ("uuid") REFERENCES "crunchbase" ("uuid");

ALTER TABLE "crunchbase_description" ADD FOREIGN KEY ("uuid") REFERENCES "crunchbase" ("uuid");

ALTER TABLE "crunchbase_categories" ADD FOREIGN KEY ("uuid") REFERENCES "crunchbase" ("uuid");

ALTER TABLE "crunchbase_category_groups" ADD FOREIGN KEY ("uuid") REFERENCES "crunchbase" ("uuid");

ALTER TABLE "crunchbase_aliases" ADD FOREIGN KEY ("uuid") REFERENCES "crunchbase" ("uuid");

ALTER TABLE "crunchbase_roles" ADD FOREIGN KEY ("uuid") REFERENCES "crunchbase" ("uuid");

ALTER TABLE "crunchbase_funding" ADD FOREIGN KEY ("uuid") REFERENCES "crunchbase" ("uuid");
