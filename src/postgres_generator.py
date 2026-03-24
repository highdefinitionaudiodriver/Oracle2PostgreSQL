"""
Oracle2PostgreSQL - PostgreSQL Code Generator
Writes the transformed SQL to output files with migration headers.
"""

import os
import shutil
from datetime import datetime
from typing import Optional

from .postgres_transformer import TransformResult


class BackupManager:
    """Creates timestamped backups of original files."""

    def __init__(self, backup_dir: Optional[str] = None):
        self.backup_dir = backup_dir

    def backup(self, filepath: str) -> Optional[str]:
        """Create a backup of the given file. Returns backup path."""
        if not os.path.exists(filepath):
            return None

        if self.backup_dir:
            backup_root = self.backup_dir
        else:
            backup_root = os.path.join(os.path.dirname(filepath), "_backup")

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_path = os.path.join(
            backup_root, timestamp, os.path.basename(filepath)
        )
        os.makedirs(os.path.dirname(backup_path), exist_ok=True)
        shutil.copy2(filepath, backup_path)
        return backup_path


class PostgresCodeGenerator:
    """Generates PostgreSQL SQL output files from TransformResult."""

    def __init__(self, output_dir: str, encoding: str = "utf-8",
                 add_header: bool = True, suffix: str = "_pg"):
        self.output_dir = output_dir
        self.encoding = encoding
        self.add_header = add_header
        self.suffix = suffix

    def generate(self, result: TransformResult) -> str:
        """Write the transformed SQL to an output file. Returns the output path."""
        os.makedirs(self.output_dir, exist_ok=True)

        # Determine output filename
        basename = os.path.basename(result.filename)
        name, ext = os.path.splitext(basename)
        if not ext:
            ext = ".sql"
        out_filename = f"{name}{self.suffix}{ext}"
        out_path = os.path.join(self.output_dir, out_filename)

        content = self._build_output(result)

        with open(out_path, "w", encoding=self.encoding, newline="\n") as f:
            f.write(content)

        return out_path

    def _build_output(self, result: TransformResult) -> str:
        """Build the complete output file content."""
        parts = []

        if self.add_header:
            parts.append(self._generate_header(result))

        # Add change annotations as comments
        if result.changes:
            parts.append(self._generate_change_summary(result))

        parts.append(result.transformed_text)

        return "\n".join(parts)

    def _generate_header(self, result: TransformResult) -> str:
        """Generate migration header comment."""
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        source = os.path.basename(result.filename) if result.filename else "unknown"

        header = (
            f"-- ======================================================================\n"
            f"-- Oracle → PostgreSQL Migration\n"
            f"-- Source: {source}\n"
            f"-- Generated: {now}\n"
            f"-- Auto-converted: {result.auto_converted}"
            f" | Review needed: {result.needs_review}"
            f" | Manual: {result.manual_only}\n"
            f"-- ======================================================================\n"
        )
        return header

    def _generate_change_summary(self, result: TransformResult) -> str:
        """Generate a comment block summarizing all changes."""
        lines = ["-- Migration Changes:"]

        for change in result.changes:
            severity_tag = f"[{change.severity}]"
            cat_tag = f"[{change.category}]"
            lines.append(f"--   {severity_tag} {cat_tag} {change.description}")

        lines.append("-- " + "=" * 68)
        lines.append("")
        return "\n".join(lines)
