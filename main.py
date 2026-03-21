"""
Oracle2PostgreSQL - Oracle to PostgreSQL Migration Tool
GUI (Tkinter) + CLI dual-mode interface.
"""

import argparse
import os
import sys
import threading
import time
from pathlib import Path

import tkinter as tk
from tkinter import ttk, filedialog, scrolledtext

from src.oracle_parser import OracleParser
from src.postgres_transformer import PostgresTransformer, TransformOptions, TransformResult
from src.postgres_generator import PostgresCodeGenerator, BackupManager
from src.report_generator import ReportGenerator
from src.i18n import I18n, LANGUAGES


# ======================================================================
# CLI Mode
# ======================================================================

def run_cli(args):
    """Run migration in CLI (headless) mode."""
    input_dir = args.input
    output_dir = args.output
    encoding = args.encoding or "utf-8"
    extensions = [e.strip() for e in (args.extensions or ".sql,.pls,.pkb,.pks,.trg,.vw,.fnc,.prc").split(",")]
    lang = args.lang or "en"

    i18n = I18n(lang)

    if not os.path.isdir(input_dir):
        print(f"Error: Input directory not found: {input_dir}")
        sys.exit(1)

    os.makedirs(output_dir, exist_ok=True)

    options = TransformOptions(
        convert_datatypes=not args.no_datatypes,
        convert_plsql=not args.no_plsql,
        convert_sequences=not args.no_sequences,
        convert_synonyms=not args.no_synonyms,
        convert_packages=not args.no_packages,
        convert_triggers=not args.no_triggers,
        generate_report=not args.no_report,
        create_backup=not args.no_backup,
        encoding=encoding,
    )

    parser = OracleParser(encoding=encoding)
    transformer = PostgresTransformer(options)
    generator = PostgresCodeGenerator(output_dir, encoding=encoding)
    backup_mgr = BackupManager() if options.create_backup else None

    results = []
    sql_files = []
    for root, dirs, files in os.walk(input_dir):
        for f in files:
            if any(f.lower().endswith(ext) for ext in extensions):
                sql_files.append(os.path.join(root, f))

    if not sql_files:
        print(f"No matching files found in {input_dir}")
        sys.exit(1)

    print(i18n.t("log_start"))
    print(f"  Files found: {len(sql_files)}")
    print("-" * 60)

    for filepath in sql_files:
        fname = os.path.basename(filepath)

        # Backup
        if backup_mgr:
            backup_mgr.backup(filepath)

        # Parse
        print(i18n.t("log_parsing", file=fname))
        schema = parser.parse_file(filepath)

        # Transform
        print(i18n.t("log_transforming", file=fname))
        result = transformer.transform(schema)

        # Generate
        print(i18n.t("log_generating", file=fname))
        out_path = generator.generate(result)
        results.append(result)

        # Status
        status = f"  Auto: {result.auto_converted}, Review: {result.needs_review}, Manual: {result.manual_only}"
        if result.errors:
            status += f", Errors: {len(result.errors)}"
        print(status)

    print("-" * 60)
    total_auto = sum(r.auto_converted for r in results)
    total_review = sum(r.needs_review for r in results)
    total_manual = sum(r.manual_only for r in results)
    print(i18n.t("log_complete",
                  total=len(results), auto=total_auto,
                  review=total_review, manual=total_manual))

    # Generate report
    if options.generate_report:
        report_gen = ReportGenerator(output_dir, lang=lang)
        report_paths = report_gen.generate(results)
        print(f"\nReport: {report_paths['html']}")
        print(f"CSV:    {report_paths['csv']}")


# ======================================================================
# GUI Mode
# ======================================================================

class Oracle2PostgreSQLApp:
    """Tkinter GUI application for Oracle → PostgreSQL migration."""

    def __init__(self, root: tk.Tk):
        self.root = root
        self.i18n = I18n("ja")
        self._cancel_flag = False
        self._running = False
        self._widgets = {}

        self._setup_window()
        self._create_widgets()
        self._apply_language()

    def _setup_window(self):
        self.root.title(self.i18n.t("app_title"))
        self.root.geometry("900x780")
        self.root.minsize(750, 650)
        self.root.configure(bg="#1e1e1e")

        style = ttk.Style()
        style.theme_use("clam")
        style.configure("TFrame", background="#1e1e1e")
        style.configure("TLabel", background="#1e1e1e", foreground="#d4d4d4",
                         font=("Consolas", 10))
        style.configure("TButton", font=("Consolas", 10))
        style.configure("TCheckbutton", background="#1e1e1e", foreground="#d4d4d4",
                         font=("Consolas", 10))
        style.configure("Header.TLabel", font=("Consolas", 12, "bold"),
                         foreground="#569cd6")
        style.configure("TLabelframe", background="#1e1e1e", foreground="#569cd6")
        style.configure("TLabelframe.Label", background="#1e1e1e", foreground="#569cd6",
                         font=("Consolas", 10, "bold"))

    def _create_widgets(self):
        main_frame = ttk.Frame(self.root, padding=10)
        main_frame.pack(fill=tk.BOTH, expand=True)

        # -- Language selector --
        lang_frame = ttk.Frame(main_frame)
        lang_frame.pack(fill=tk.X, pady=(0, 5))
        self._widgets["lang_label"] = ttk.Label(lang_frame, text="言語:")
        self._widgets["lang_label"].pack(side=tk.LEFT)
        self.lang_var = tk.StringVar(value="ja - 日本語")
        lang_names = [f"{code} - {name}" for code, name in LANGUAGES.items()]
        self.lang_combo = ttk.Combobox(lang_frame, textvariable=self.lang_var,
                                        values=lang_names, state="readonly", width=30)
        self.lang_combo.pack(side=tk.LEFT, padx=(5, 0))
        self.lang_combo.bind("<<ComboboxSelected>>", self._on_language_change)

        # -- Input/Output folders --
        io_frame = ttk.Frame(main_frame)
        io_frame.pack(fill=tk.X, pady=5)

        self._widgets["input_label"] = ttk.Label(io_frame, text="入力フォルダ（Oracle SQL）:")
        self._widgets["input_label"].grid(row=0, column=0, sticky=tk.W, pady=2)
        self.input_var = tk.StringVar()
        self.input_entry = ttk.Entry(io_frame, textvariable=self.input_var, width=60)
        self.input_entry.grid(row=0, column=1, padx=5, pady=2, sticky=tk.EW)
        self._widgets["input_browse"] = ttk.Button(io_frame, text="参照...",
                                                     command=self._browse_input)
        self._widgets["input_browse"].grid(row=0, column=2, pady=2)

        self._widgets["output_label"] = ttk.Label(io_frame, text="出力フォルダ（PostgreSQL）:")
        self._widgets["output_label"].grid(row=1, column=0, sticky=tk.W, pady=2)
        self.output_var = tk.StringVar()
        self.output_entry = ttk.Entry(io_frame, textvariable=self.output_var, width=60)
        self.output_entry.grid(row=1, column=1, padx=5, pady=2, sticky=tk.EW)
        self._widgets["output_browse"] = ttk.Button(io_frame, text="参照...",
                                                      command=self._browse_output)
        self._widgets["output_browse"].grid(row=1, column=2, pady=2)
        io_frame.columnconfigure(1, weight=1)

        # -- Settings row --
        settings_frame = ttk.Frame(main_frame)
        settings_frame.pack(fill=tk.X, pady=5)

        self._widgets["encoding_label"] = ttk.Label(settings_frame, text="エンコーディング:")
        self._widgets["encoding_label"].pack(side=tk.LEFT)
        self.encoding_var = tk.StringVar(value="utf-8")
        enc_combo = ttk.Combobox(settings_frame, textvariable=self.encoding_var,
                                  values=["utf-8", "shift_jis", "euc-jp", "iso-8859-1",
                                          "cp1252", "latin1"],
                                  state="readonly", width=15)
        enc_combo.pack(side=tk.LEFT, padx=(5, 20))

        self._widgets["ext_label"] = ttk.Label(settings_frame, text="ファイル拡張子:")
        self._widgets["ext_label"].pack(side=tk.LEFT)
        self.ext_var = tk.StringVar(value=".sql,.pls,.pkb,.pks,.trg,.vw,.fnc,.prc")
        ext_entry = ttk.Entry(settings_frame, textvariable=self.ext_var, width=40)
        ext_entry.pack(side=tk.LEFT, padx=5)

        # -- Migration options --
        self._widgets["options_frame"] = ttk.LabelFrame(main_frame, text="移行オプション",
                                                         padding=10)
        self._widgets["options_frame"].pack(fill=tk.X, pady=5)

        opts = self._widgets["options_frame"]
        self.opt_datatypes = tk.BooleanVar(value=True)
        self.opt_plsql = tk.BooleanVar(value=True)
        self.opt_sequences = tk.BooleanVar(value=True)
        self.opt_synonyms = tk.BooleanVar(value=True)
        self.opt_packages = tk.BooleanVar(value=True)
        self.opt_triggers = tk.BooleanVar(value=True)
        self.opt_report = tk.BooleanVar(value=True)
        self.opt_backup = tk.BooleanVar(value=True)

        self._widgets["cb_datatypes"] = ttk.Checkbutton(opts, text="データ型変換",
                                                          variable=self.opt_datatypes)
        self._widgets["cb_plsql"] = ttk.Checkbutton(opts, text="PL/SQL → PL/pgSQL 変換",
                                                      variable=self.opt_plsql)
        self._widgets["cb_sequences"] = ttk.Checkbutton(opts, text="シーケンス変換",
                                                          variable=self.opt_sequences)
        self._widgets["cb_synonyms"] = ttk.Checkbutton(opts, text="シノニム → ビュー/search_path 変換",
                                                         variable=self.opt_synonyms)
        self._widgets["cb_packages"] = ttk.Checkbutton(opts, text="パッケージ → スキーマ 変換",
                                                         variable=self.opt_packages)
        self._widgets["cb_triggers"] = ttk.Checkbutton(opts, text="トリガー変換",
                                                         variable=self.opt_triggers)
        self._widgets["cb_report"] = ttk.Checkbutton(opts, text="移行レポート生成",
                                                       variable=self.opt_report)
        self._widgets["cb_backup"] = ttk.Checkbutton(opts, text="バックアップ作成",
                                                       variable=self.opt_backup)

        # Grid layout for checkboxes (2 columns)
        checkboxes = [
            "cb_datatypes", "cb_plsql", "cb_sequences", "cb_synonyms",
            "cb_packages", "cb_triggers", "cb_report", "cb_backup",
        ]
        for i, key in enumerate(checkboxes):
            self._widgets[key].grid(row=i // 2, column=i % 2, sticky=tk.W, padx=10, pady=2)

        # -- Buttons --
        btn_frame = ttk.Frame(main_frame)
        btn_frame.pack(fill=tk.X, pady=5)

        self._widgets["convert_btn"] = ttk.Button(btn_frame, text="変換",
                                                    command=self._start_conversion)
        self._widgets["convert_btn"].pack(side=tk.LEFT, padx=5)
        self._widgets["cancel_btn"] = ttk.Button(btn_frame, text="キャンセル",
                                                   command=self._cancel_conversion,
                                                   state=tk.DISABLED)
        self._widgets["cancel_btn"].pack(side=tk.LEFT, padx=5)

        # -- Progress bar --
        self.progress_var = tk.DoubleVar(value=0)
        self.progress_bar = ttk.Progressbar(main_frame, variable=self.progress_var,
                                             maximum=100)
        self.progress_bar.pack(fill=tk.X, pady=5)

        # -- Log output --
        self.log_text = scrolledtext.ScrolledText(
            main_frame, height=18, bg="#1e1e1e", fg="#d4d4d4",
            font=("Consolas", 10), insertbackground="#d4d4d4",
            selectbackground="#264f78", wrap=tk.WORD,
        )
        self.log_text.pack(fill=tk.BOTH, expand=True, pady=5)

        # Log tags
        self.log_text.tag_configure("info", foreground="#4ec9b0")
        self.log_text.tag_configure("success", foreground="#6a9955")
        self.log_text.tag_configure("warning", foreground="#dcdcaa")
        self.log_text.tag_configure("error", foreground="#f44747")
        self.log_text.tag_configure("header", foreground="#569cd6", font=("Consolas", 10, "bold"))

    def _apply_language(self):
        """Update all widget text for the current language."""
        t = self.i18n.t
        self.root.title(t("app_title"))
        self._widgets["lang_label"].configure(text=t("language"))
        self._widgets["input_label"].configure(text=t("input_folder"))
        self._widgets["output_label"].configure(text=t("output_folder"))
        self._widgets["input_browse"].configure(text=t("browse"))
        self._widgets["output_browse"].configure(text=t("browse"))
        self._widgets["encoding_label"].configure(text=t("encoding"))
        self._widgets["ext_label"].configure(text=t("file_extensions"))
        self._widgets["options_frame"].configure(text=t("options"))
        self._widgets["cb_datatypes"].configure(text=t("convert_datatypes"))
        self._widgets["cb_plsql"].configure(text=t("convert_plsql"))
        self._widgets["cb_sequences"].configure(text=t("convert_sequences"))
        self._widgets["cb_synonyms"].configure(text=t("convert_synonyms"))
        self._widgets["cb_packages"].configure(text=t("convert_packages"))
        self._widgets["cb_triggers"].configure(text=t("convert_triggers"))
        self._widgets["cb_report"].configure(text=t("generate_report"))
        self._widgets["cb_backup"].configure(text=t("create_backup"))
        self._widgets["convert_btn"].configure(text=t("convert"))
        self._widgets["cancel_btn"].configure(text=t("cancel"))

    def _on_language_change(self, event=None):
        selected = self.lang_var.get()
        lang_code = selected.split(" - ")[0]
        self.i18n.set_language(lang_code)
        self._apply_language()

    def _browse_input(self):
        path = filedialog.askdirectory(title="Select Input Folder")
        if path:
            self.input_var.set(path)

    def _browse_output(self):
        path = filedialog.askdirectory(title="Select Output Folder")
        if path:
            self.output_var.set(path)

    def _log(self, message: str, tag: str = "info"):
        self.log_text.insert(tk.END, message + "\n", tag)
        self.log_text.see(tk.END)

    def _start_conversion(self):
        input_dir = self.input_var.get().strip()
        output_dir = self.output_var.get().strip()

        if not input_dir or not os.path.isdir(input_dir):
            self._log(self.i18n.t("log_error", message="Invalid input folder"), "error")
            return
        if not output_dir:
            self._log(self.i18n.t("log_error", message="Output folder not specified"), "error")
            return

        self._running = True
        self._cancel_flag = False
        self._widgets["convert_btn"].configure(state=tk.DISABLED)
        self._widgets["cancel_btn"].configure(state=tk.NORMAL)
        self.progress_var.set(0)
        self.log_text.delete("1.0", tk.END)

        thread = threading.Thread(target=self._run_conversion,
                                   args=(input_dir, output_dir), daemon=True)
        thread.start()

    def _cancel_conversion(self):
        self._cancel_flag = True

    def _run_conversion(self, input_dir: str, output_dir: str):
        """Background conversion thread."""
        try:
            t = self.i18n.t
            encoding = self.encoding_var.get()
            extensions = [e.strip() for e in self.ext_var.get().split(",")]

            options = TransformOptions(
                convert_datatypes=self.opt_datatypes.get(),
                convert_plsql=self.opt_plsql.get(),
                convert_sequences=self.opt_sequences.get(),
                convert_synonyms=self.opt_synonyms.get(),
                convert_packages=self.opt_packages.get(),
                convert_triggers=self.opt_triggers.get(),
                generate_report=self.opt_report.get(),
                create_backup=self.opt_backup.get(),
                encoding=encoding,
            )

            self.root.after(0, self._log, t("log_start"), "header")

            parser = OracleParser(encoding=encoding)
            transformer = PostgresTransformer(options)
            generator = PostgresCodeGenerator(output_dir, encoding=encoding)
            backup_mgr = BackupManager() if options.create_backup else None

            # Find files
            sql_files = []
            for root, dirs, files in os.walk(input_dir):
                for f in files:
                    if any(f.lower().endswith(ext) for ext in extensions):
                        sql_files.append(os.path.join(root, f))

            if not sql_files:
                self.root.after(0, self._log,
                                t("log_error", message="No matching files found"), "error")
                return

            self.root.after(0, self._log, f"  Files: {len(sql_files)}", "info")
            results = []

            for idx, filepath in enumerate(sql_files):
                if self._cancel_flag:
                    self.root.after(0, self._log, "Cancelled.", "warning")
                    break

                fname = os.path.basename(filepath)
                progress = (idx / len(sql_files)) * 100
                self.root.after(0, self.progress_var.set, progress)

                # Backup
                if backup_mgr:
                    backup_mgr.backup(filepath)

                # Parse
                self.root.after(0, self._log, t("log_parsing", file=fname), "info")
                schema = parser.parse_file(filepath)

                if schema.errors:
                    for err in schema.errors:
                        self.root.after(0, self._log,
                                         t("log_warning", message=err), "warning")

                # Transform
                self.root.after(0, self._log, t("log_transforming", file=fname), "info")
                result = transformer.transform(schema)

                # Generate
                self.root.after(0, self._log, t("log_generating", file=fname), "info")
                generator.generate(result)
                results.append(result)

                # Status
                status = (f"  Auto: {result.auto_converted}, "
                          f"Review: {result.needs_review}, "
                          f"Manual: {result.manual_only}")
                tag = "success" if result.manual_only == 0 else "warning"
                self.root.after(0, self._log, status, tag)

            if not self._cancel_flag:
                self.root.after(0, self.progress_var.set, 100)

                total_auto = sum(r.auto_converted for r in results)
                total_review = sum(r.needs_review for r in results)
                total_manual = sum(r.manual_only for r in results)

                self.root.after(0, self._log,
                                 t("log_complete", total=len(results),
                                   auto=total_auto, review=total_review,
                                   manual=total_manual), "success")

                # Generate report
                if options.generate_report and results:
                    report_gen = ReportGenerator(output_dir, lang=self.i18n.lang_code)
                    report_paths = report_gen.generate(results)
                    self.root.after(0, self._log,
                                     f"  Report: {report_paths['html']}", "success")

        except Exception as e:
            self.root.after(0, self._log,
                             self.i18n.t("log_error", message=str(e)), "error")
        finally:
            self._running = False
            self.root.after(0, self._widgets["convert_btn"].configure, {"state": tk.NORMAL})
            self.root.after(0, self._widgets["cancel_btn"].configure, {"state": tk.DISABLED})


# ======================================================================
# Entry Point
# ======================================================================

def main():
    parser = argparse.ArgumentParser(
        description="Oracle to PostgreSQL Migration Tool"
    )
    parser.add_argument("-i", "--input", help="Input directory containing Oracle SQL files")
    parser.add_argument("-o", "--output", help="Output directory for PostgreSQL files")
    parser.add_argument("-e", "--encoding", default="utf-8", help="File encoding (default: utf-8)")
    parser.add_argument("--extensions", default=".sql,.pls,.pkb,.pks,.trg,.vw,.fnc,.prc",
                         help="Comma-separated file extensions to process")
    parser.add_argument("--lang", default="en", help="Language for output (default: en)")
    parser.add_argument("--no-datatypes", action="store_true", help="Skip data type conversion")
    parser.add_argument("--no-plsql", action="store_true", help="Skip PL/SQL conversion")
    parser.add_argument("--no-sequences", action="store_true", help="Skip sequence conversion")
    parser.add_argument("--no-synonyms", action="store_true", help="Skip synonym conversion")
    parser.add_argument("--no-packages", action="store_true", help="Skip package conversion")
    parser.add_argument("--no-triggers", action="store_true", help="Skip trigger conversion")
    parser.add_argument("--no-report", action="store_true", help="Skip report generation")
    parser.add_argument("--no-backup", action="store_true", help="Skip backup creation")

    args = parser.parse_args()

    if args.input and args.output:
        run_cli(args)
    else:
        root = tk.Tk()
        app = Oracle2PostgreSQLApp(root)
        root.mainloop()


if __name__ == "__main__":
    main()
