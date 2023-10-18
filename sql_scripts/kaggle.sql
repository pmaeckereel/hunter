CREATE TABLE "kaggle" (
  "uuid" uuid PRIMARY KEY,
  "company" varchar,
  "num_of_employees" integer,
  "sector" varchar,
  "city" varchar,
  "state" varchar,
  "newcomer" bool,
  "ceo_founder" bool,
  "ceo_woman" bool,
  "ceo" varchar,
  "website" varchar,
  "created_at" timestamp,
  "updated_at" timestamp
);

CREATE TABLE "kaggle_financial_infos" (
  "uuid" uuid PRIMARY KEY,
  "revenue" float,
  "profit" float,
  "profitable" bool,
  "ticker" varchar,
  "market_cap" float,
  "created_at" timestamp,
  "updated_at" timestamp
);

ALTER TABLE "kaggle_financial_infos" ADD FOREIGN KEY ("uuid") REFERENCES "kaggle" ("uuid");
