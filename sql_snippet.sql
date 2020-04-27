CREATE TABLE pratilipi_scripts
(
    id              INT AUTO_INCREMENT,
    title           TEXT,
    read_count      INT       DEFAULT 0,
    language        VARCHAR(50),
    rating          FLOAT     DEFAULT NULL,
    author_id       BIGINT DEFAULT NULL,
    pratilipi_id    BIGINT,
    page_url        TEXT,
    site_updated_at TIMESTAMP DEFAULT NULL,
    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (id),
    CONSTRAINT fk_author FOREIGN KEY (author_id)
        REFERENCES pratilipi_authors (pratilipi_id)
);

CREATE TABLE pratilipi_authors
(
    id                   INT AUTO_INCREMENT,
    name                 VARCHAR(100),
    follow_count         INT         DEFAULT 0,
    read_count           INT         DEFAULT 0,
    language             VARCHAR(50) DEFAULT NULL,
    gender               VARCHAR(10) DEFAULT NULL,
    pratilipi_id         BIGINT,
    page_url             TEXT,
    site_registration_at TIMESTAMP   DEFAULT NULL,
    created_at           TIMESTAMP   DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (id),
    UNIQUE KEY (pratilipi_id)
);