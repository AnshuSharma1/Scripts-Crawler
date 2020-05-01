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

CREATE TABLE pratilipi_scripts
(
    id              INT AUTO_INCREMENT,
    title           TEXT,
    read_count      INT       DEFAULT 0,
    read_time       INT       DEFAULT 0,
    tags            TEXT,
    language        VARCHAR(50),
    rating          FLOAT     DEFAULT NULL,
    author_id       BIGINT    DEFAULT NULL,
    pratilipi_id    BIGINT,
    page_url        TEXT,
    site_updated_at TIMESTAMP DEFAULT NULL,
    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (id),
    UNIQUE KEY (pratilipi_id)
);

CREATE TABLE pratilipi_categories
(
    id           INT AUTO_INCREMENT,
    pratilipi_id BIGINT,
    category     VARCHAR(100),
    created_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (id),
    CONSTRAINT fk_pratilipi FOREIGN KEY (pratilipi_id)
        REFERENCES pratilipi_scripts (pratilipi_id)
);