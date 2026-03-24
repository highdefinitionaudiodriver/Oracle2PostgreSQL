"""
Oracle2PostgreSQL - Configuration Loader
Loads config.yaml and produces TransformOptions + logger settings.
"""

import os
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Set

import yaml

from .postgres_transformer import TransformOptions


@dataclass
class LoggingConfig:
    """Logging configuration extracted from config.yaml."""
    console_level: str = "INFO"
    file_level: str = "DEBUG"
    log_file: str = "logs/migration.log"
    max_bytes: int = 5 * 1024 * 1024
    backup_count: int = 3


@dataclass
class AppConfig:
    """Complete application configuration."""
    # General
    input_encoding: str = "utf-8"
    output_encoding: str = "utf-8"
    file_extensions: List[str] = field(default_factory=lambda: [
        ".sql", ".pls", ".pkb", ".pks", ".trg", ".vw", ".fnc", ".prc"
    ])
    output_suffix: str = "_pg"
    add_header: bool = True
    create_backup: bool = True
    backup_dir: Optional[str] = None

    # Language
    language: str = "ja"

    # Rule category toggles
    category_toggles: Dict[str, bool] = field(default_factory=lambda: {
        "DATATYPE": True, "FUNCTION": True, "SYNTAX": True, "PLSQL": True,
        "SEQUENCE": True, "OBJECT": True, "TRIGGER": True, "SYNONYM": True,
    })
    disabled_rules: Set[str] = field(default_factory=set)
    forced_rules: Set[str] = field(default_factory=set)

    # Schema mapping
    schema_mapping: Dict[str, str] = field(default_factory=dict)

    # Logging
    logging: LoggingConfig = field(default_factory=LoggingConfig)

    # Report
    report_html: bool = True
    report_csv: bool = True
    report_language: Optional[str] = None

    def to_transform_options(self) -> TransformOptions:
        """Convert to TransformOptions for the transformer engine."""
        return TransformOptions(
            convert_datatypes=self.category_toggles.get("DATATYPE", True),
            convert_plsql=self.category_toggles.get("PLSQL", True),
            convert_sequences=self.category_toggles.get("SEQUENCE", True),
            convert_synonyms=self.category_toggles.get("SYNONYM", True),
            convert_packages=self.category_toggles.get("PLSQL", True),
            convert_triggers=self.category_toggles.get("TRIGGER", True),
            generate_report=self.report_html or self.report_csv,
            create_backup=self.create_backup,
            encoding=self.input_encoding,
        )


def load_config(config_path: str) -> AppConfig:
    """Load configuration from a YAML file and return AppConfig."""
    if not os.path.isfile(config_path):
        raise FileNotFoundError(f"Config file not found: {config_path}")

    with open(config_path, "r", encoding="utf-8") as f:
        raw = yaml.safe_load(f) or {}

    cfg = AppConfig()

    # General
    general = raw.get("general", {})
    cfg.input_encoding = general.get("input_encoding", cfg.input_encoding)
    cfg.output_encoding = general.get("output_encoding", cfg.output_encoding)
    if "file_extensions" in general:
        cfg.file_extensions = general["file_extensions"]
    cfg.output_suffix = general.get("output_suffix", cfg.output_suffix)
    cfg.add_header = general.get("add_header", cfg.add_header)
    cfg.create_backup = general.get("create_backup", cfg.create_backup)
    cfg.backup_dir = general.get("backup_dir", cfg.backup_dir)

    # Language
    cfg.language = raw.get("language", cfg.language)

    # Rules
    rules_section = raw.get("rules", {})
    categories = rules_section.get("categories", {})
    for cat, enabled in categories.items():
        cfg.category_toggles[cat.upper()] = bool(enabled)
    cfg.disabled_rules = set(rules_section.get("disabled_rules", []))
    cfg.forced_rules = set(rules_section.get("forced_rules", []))

    # Schema mapping
    cfg.schema_mapping = raw.get("schema_mapping", {})

    # Logging
    log_section = raw.get("logging", {})
    cfg.logging = LoggingConfig(
        console_level=log_section.get("console_level", "INFO"),
        file_level=log_section.get("file_level", "DEBUG"),
        log_file=log_section.get("log_file", "logs/migration.log"),
        max_bytes=log_section.get("max_bytes", 5 * 1024 * 1024),
        backup_count=log_section.get("backup_count", 3),
    )

    # Report
    report_section = raw.get("report", {})
    cfg.report_html = report_section.get("html", True)
    cfg.report_csv = report_section.get("csv", True)
    cfg.report_language = report_section.get("language", None)

    return cfg


def get_default_config() -> AppConfig:
    """Return default configuration without loading a file."""
    return AppConfig()
