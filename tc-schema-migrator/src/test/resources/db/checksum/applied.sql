--liquibase formatted sql

--changeset tc:checksum-probe logicalFilePath:db/checksum/changelog.sql
SELECT 1;
