DROP TABLE IF EXISTS test_float64;

CREATE TABLE test_float64 (
    idx INTEGER,
    res DOUBLE
);

INSERT INTO test_float64 (idx, res) VALUES (1, 3.14159265358979D);
INSERT INTO test_float64 (idx, res) VALUES (2, 0.0D);
INSERT INTO test_float64 (idx, res) VALUES (3, -1.7976931348623157e308D);
INSERT INTO test_float64 (idx, res) VALUES (4, 1.7976931348623157e308D);
INSERT INTO test_float64 (idx, res) VALUES (5, 2.2250738585072014e-308D);
INSERT INTO test_float64 (idx, res) VALUES (6, NULL);
