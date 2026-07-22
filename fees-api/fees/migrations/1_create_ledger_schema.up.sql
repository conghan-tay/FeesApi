CREATE TABLE bills (
    bill_id   TEXT PRIMARY KEY,
    client_id TEXT        NOT NULL,
    currency  CHAR(3)     NOT NULL,
    period    TEXT        NOT NULL,
    status    TEXT        NOT NULL DEFAULT 'OPEN'
              CONSTRAINT bills_status_check CHECK (status IN ('OPEN', 'CLOSED')),
    opened_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    closed_at TIMESTAMPTZ,

    CONSTRAINT bills_client_currency_period_unique UNIQUE (client_id, currency, period)
);

CREATE INDEX idx_bills_client_status ON bills (client_id, status);
CREATE INDEX idx_bills_period ON bills (period);
CREATE INDEX idx_bills_currency ON bills (currency);

CREATE TABLE line_items (
    id           BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    bill_id      TEXT        NOT NULL REFERENCES bills(bill_id),
    reference    TEXT        NOT NULL,
    amount_minor BIGINT      NOT NULL,
    currency     CHAR(3)     NOT NULL,
    fee_type     TEXT        NOT NULL,
    description  TEXT        NOT NULL DEFAULT '',
    applied_at   TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT line_items_bill_reference_unique UNIQUE (bill_id, reference)
);

CREATE INDEX idx_line_items_bill ON line_items (bill_id);

CREATE TABLE currencies (
    code         CHAR(3) PRIMARY KEY,
    exponent     SMALLINT NOT NULL,
    display_name TEXT     NOT NULL DEFAULT ''
);

INSERT INTO currencies (code, exponent, display_name)
VALUES
    ('GEL', 2, 'Georgian Lari'),
    ('USD', 2, 'US Dollar');
