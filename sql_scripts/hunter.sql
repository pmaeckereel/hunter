CREATE TABLE "hunter" (
  "id" integer PRIMARY KEY,
  "domain" varchar,
  "generic_emails" integer,
  "personal_emails" integer,
  "last_crawl" date,
  "language" varchar,
  "company_name" varchar,
  "country" varchar,
  "employees_count" integer,
  "created_at" timestamp,
  "updated_at" timestamp
);

CREATE TABLE "hunter_contact" (
  "id" integer PRIMARY KEY,
  "twitter_url" varchar,
  "facebook_url" varchar,
  "instagram_url" varchar,
  "linkedin_url" varchar,
  "youtube_url" varchar,
  "phone" varchar,
  "apple_app" varchar,
  "google_play" varchar,
  "state" varchar,
  "city" varchar,
  "street" varchar,
  "postcode" varchar,
  "created_at" timestamp,
  "updated_at" timestamp
);

CREATE TABLE "hunter_categories" (
  "id" integer PRIMARY KEY,
  "category" varchar,
  "created_at" timestamp,
  "updated_at" timestamp
);

CREATE TABLE "hunter_description" (
  "id" integer PRIMARY KEY,
  "description" varchar,
  "created_at" timestamp,
  "updated_at" timestamp
);

ALTER TABLE "hunter_contact" ADD FOREIGN KEY ("id") REFERENCES "hunter" ("id");

ALTER TABLE "hunter_categories" ADD FOREIGN KEY ("id") REFERENCES "hunter" ("id");

ALTER TABLE "hunter_description" ADD FOREIGN KEY ("id") REFERENCES "hunter" ("id");
