CREATE TABLE `rate` (
  `id` bigint NOT NULL,
  `ccn` varchar(10) NOT NULL,
  `code` varchar(64),
  `code_prefix` varchar(32),
  `code_orig` varchar(255),
  `modifier` varchar(64),
  `ndc` varchar(64),
  `apc` varchar(32),
  `rev_code` varchar(32),
  `internal_code` varchar(64),
  `billing_class` varchar(32),
  `patient_class` varchar(20),
  `payer_orig` varchar(255),
  `plan_orig` varchar(255),
  `payer_category` varchar(20),
  `standard_charge` decimal(12,2),
  `rate_percent` decimal(8,4),
  `drug_unit` varchar(32),
  `drug_quantity` varchar(32),
  PRIMARY KEY (`id`),
  KEY `idx_rate_ccn` (`ccn`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin;

CREATE TABLE `hospital` (
  `ccn` varchar(10) NOT NULL,
  `hospital_name` varchar(255),
  `state` char(2),
  `file_url` varchar(4096),
  `transparency_page` varchar(4096),
  PRIMARY KEY (`ccn`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin;
