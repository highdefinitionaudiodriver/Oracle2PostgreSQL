"""
Oracle2PostgreSQL - Internationalization Module
Supports 59 languages for GUI/CLI interface.
"""


LANGUAGES = {
    "en": "English",
    "ja": "日本語",
    "zh_CN": "简体中文",
    "zh_TW": "繁體中文",
    "ko": "한국어",
    "fr": "Français",
    "de": "Deutsch",
    "es": "Español",
    "pt": "Português",
    "pt_BR": "Português (Brasil)",
    "it": "Italiano",
    "nl": "Nederlands",
    "ru": "Русский",
    "pl": "Polski",
    "cs": "Čeština",
    "sk": "Slovenčina",
    "hu": "Magyar",
    "ro": "Română",
    "bg": "Български",
    "hr": "Hrvatski",
    "sr": "Srpski",
    "sl": "Slovenščina",
    "uk": "Українська",
    "el": "Ελληνικά",
    "tr": "Türkçe",
    "ar": "العربية",
    "he": "עברית",
    "fa": "فارسی",
    "hi": "हिन्दी",
    "bn": "বাংলা",
    "ta": "தமிழ்",
    "te": "తెలుగు",
    "th": "ไทย",
    "vi": "Tiếng Việt",
    "id": "Bahasa Indonesia",
    "ms": "Bahasa Melayu",
    "tl": "Filipino",
    "sw": "Kiswahili",
    "fi": "Suomi",
    "sv": "Svenska",
    "no": "Norsk",
    "da": "Dansk",
    "is": "Íslenska",
    "et": "Eesti",
    "lv": "Latviešu",
    "lt": "Lietuvių",
    "ga": "Gaeilge",
    "cy": "Cymraeg",
    "eu": "Euskara",
    "ca": "Català",
    "gl": "Galego",
    "af": "Afrikaans",
    "sq": "Shqip",
    "mk": "Македонски",
    "bs": "Bosanski",
    "mt": "Malti",
    "lb": "Lëtzebuergesch",
    "ka": "ქართული",
    "hy": "Հայերեն",
    "az": "Azərbaycan",
    "kk": "Қазақша",
    "uz": "Oʻzbekcha",
}

TRANSLATIONS = {
    "app_title": {
        "en": "Oracle to PostgreSQL Migration Tool",
        "ja": "Oracle→PostgreSQL 移行ツール",
        "zh_CN": "Oracle到PostgreSQL迁移工具",
        "zh_TW": "Oracle到PostgreSQL遷移工具",
        "ko": "Oracle에서 PostgreSQL 마이그레이션 도구",
        "fr": "Outil de migration Oracle vers PostgreSQL",
        "de": "Oracle zu PostgreSQL Migrationswerkzeug",
        "es": "Herramienta de migración Oracle a PostgreSQL",
        "pt": "Ferramenta de migração Oracle para PostgreSQL",
        "it": "Strumento di migrazione Oracle a PostgreSQL",
        "ru": "Инструмент миграции Oracle в PostgreSQL",
    },
    "input_folder": {
        "en": "Input Folder (Oracle SQL):",
        "ja": "入力フォルダ（Oracle SQL）:",
        "zh_CN": "输入文件夹（Oracle SQL）:",
        "ko": "입력 폴더 (Oracle SQL):",
        "fr": "Dossier d'entrée (Oracle SQL) :",
        "de": "Eingabeordner (Oracle SQL):",
        "es": "Carpeta de entrada (Oracle SQL):",
    },
    "output_folder": {
        "en": "Output Folder (PostgreSQL):",
        "ja": "出力フォルダ（PostgreSQL）:",
        "zh_CN": "输出文件夹（PostgreSQL）:",
        "ko": "출력 폴더 (Oracle SQL):",
        "fr": "Dossier de sortie (PostgreSQL) :",
        "de": "Ausgabeordner (PostgreSQL):",
        "es": "Carpeta de salida (PostgreSQL):",
    },
    "browse": {
        "en": "Browse...",
        "ja": "参照...",
        "zh_CN": "浏览...",
        "ko": "찾아보기...",
        "fr": "Parcourir...",
        "de": "Durchsuchen...",
        "es": "Explorar...",
    },
    "convert": {
        "en": "Convert",
        "ja": "変換",
        "zh_CN": "转换",
        "ko": "변환",
        "fr": "Convertir",
        "de": "Konvertieren",
        "es": "Convertir",
    },
    "cancel": {
        "en": "Cancel",
        "ja": "キャンセル",
        "zh_CN": "取消",
        "ko": "취소",
        "fr": "Annuler",
        "de": "Abbrechen",
        "es": "Cancelar",
    },
    "language": {
        "en": "Language:",
        "ja": "言語:",
        "zh_CN": "语言:",
        "ko": "언어:",
        "fr": "Langue :",
        "de": "Sprache:",
        "es": "Idioma:",
    },
    "encoding": {
        "en": "Encoding:",
        "ja": "エンコーディング:",
        "zh_CN": "编码:",
        "ko": "인코딩:",
        "fr": "Encodage :",
        "de": "Kodierung:",
        "es": "Codificación:",
    },
    "file_extensions": {
        "en": "File Extensions:",
        "ja": "ファイル拡張子:",
        "zh_CN": "文件扩展名:",
        "ko": "파일 확장자:",
        "fr": "Extensions de fichier :",
        "de": "Dateierweiterungen:",
        "es": "Extensiones de archivo:",
    },
    "options": {
        "en": "Migration Options",
        "ja": "移行オプション",
        "zh_CN": "迁移选项",
        "ko": "마이그레이션 옵션",
        "fr": "Options de migration",
        "de": "Migrationsoptionen",
        "es": "Opciones de migración",
    },
    "convert_datatypes": {
        "en": "Convert Data Types",
        "ja": "データ型変換",
        "zh_CN": "转换数据类型",
        "ko": "데이터 유형 변환",
        "fr": "Convertir les types de données",
        "de": "Datentypen konvertieren",
        "es": "Convertir tipos de datos",
    },
    "convert_plsql": {
        "en": "Convert PL/SQL → PL/pgSQL",
        "ja": "PL/SQL → PL/pgSQL 変換",
        "zh_CN": "转换PL/SQL → PL/pgSQL",
        "ko": "PL/SQL → PL/pgSQL 변환",
        "fr": "Convertir PL/SQL → PL/pgSQL",
        "de": "PL/SQL → PL/pgSQL konvertieren",
        "es": "Convertir PL/SQL → PL/pgSQL",
    },
    "convert_sequences": {
        "en": "Convert Sequences",
        "ja": "シーケンス変換",
        "zh_CN": "转换序列",
        "ko": "시퀀스 변환",
        "fr": "Convertir les séquences",
        "de": "Sequenzen konvertieren",
        "es": "Convertir secuencias",
    },
    "convert_synonyms": {
        "en": "Convert Synonyms → Views/Search Path",
        "ja": "シノニム → ビュー/search_path 変換",
        "zh_CN": "转换同义词 → 视图/搜索路径",
        "ko": "동의어 → 뷰/검색 경로 변환",
    },
    "convert_packages": {
        "en": "Convert Packages → Schemas",
        "ja": "パッケージ → スキーマ 変換",
        "zh_CN": "转换包 → 模式",
        "ko": "패키지 → 스키마 변환",
    },
    "convert_triggers": {
        "en": "Convert Triggers",
        "ja": "トリガー変換",
        "zh_CN": "转换触发器",
        "ko": "트리거 변환",
    },
    "generate_report": {
        "en": "Generate Migration Report",
        "ja": "移行レポート生成",
        "zh_CN": "生成迁移报告",
        "ko": "마이그레이션 보고서 생성",
    },
    "create_backup": {
        "en": "Create Backup",
        "ja": "バックアップ作成",
        "zh_CN": "创建备份",
        "ko": "백업 생성",
    },
    "log_start": {
        "en": "Starting Oracle → PostgreSQL migration...",
        "ja": "Oracle → PostgreSQL 移行を開始しています...",
        "zh_CN": "开始Oracle → PostgreSQL迁移...",
        "ko": "Oracle → PostgreSQL 마이그레이션 시작 중...",
    },
    "log_parsing": {
        "en": "Parsing: {file}",
        "ja": "解析中: {file}",
        "zh_CN": "正在解析: {file}",
        "ko": "파싱 중: {file}",
    },
    "log_transforming": {
        "en": "Transforming: {file}",
        "ja": "変換中: {file}",
        "zh_CN": "正在转换: {file}",
        "ko": "변환 중: {file}",
    },
    "log_generating": {
        "en": "Generating: {file}",
        "ja": "生成中: {file}",
        "zh_CN": "正在生成: {file}",
        "ko": "생성 중: {file}",
    },
    "log_complete": {
        "en": "Migration complete! Files: {total}, Auto: {auto}, Review: {review}, Manual: {manual}",
        "ja": "移行完了！ファイル数: {total}, 自動: {auto}, 要確認: {review}, 手動: {manual}",
        "zh_CN": "迁移完成！文件: {total}, 自动: {auto}, 需审查: {review}, 手动: {manual}",
        "ko": "마이그레이션 완료! 파일: {total}, 자동: {auto}, 검토: {review}, 수동: {manual}",
    },
    "log_error": {
        "en": "Error: {message}",
        "ja": "エラー: {message}",
        "zh_CN": "错误: {message}",
        "ko": "오류: {message}",
    },
    "log_warning": {
        "en": "Warning: {message}",
        "ja": "警告: {message}",
        "zh_CN": "警告: {message}",
        "ko": "경고: {message}",
    },
    "log_skip": {
        "en": "Skipped (not Oracle SQL): {file}",
        "ja": "スキップ（Oracle SQLではありません）: {file}",
        "zh_CN": "跳过（非Oracle SQL）: {file}",
        "ko": "건너뜀 (Oracle SQL 아님): {file}",
    },
    "report_title": {
        "en": "Oracle to PostgreSQL Migration Report",
        "ja": "Oracle→PostgreSQL 移行レポート",
        "zh_CN": "Oracle到PostgreSQL迁移报告",
        "ko": "Oracle에서 PostgreSQL 마이그레이션 보고서",
    },
    "report_summary": {
        "en": "Executive Summary",
        "ja": "概要サマリー",
        "zh_CN": "执行摘要",
        "ko": "요약",
    },
    "report_details": {
        "en": "Detailed Changes",
        "ja": "変更詳細",
        "zh_CN": "详细变更",
        "ko": "상세 변경 사항",
    },
    "severity_auto": {
        "en": "AUTO (Automatically converted)",
        "ja": "AUTO（自動変換済み）",
        "zh_CN": "自动（已自动转换）",
        "ko": "자동 (자동 변환됨)",
    },
    "severity_review": {
        "en": "REVIEW (Needs verification)",
        "ja": "REVIEW（要確認）",
        "zh_CN": "审查（需要验证）",
        "ko": "검토 (확인 필요)",
    },
    "severity_manual": {
        "en": "MANUAL (Requires manual intervention)",
        "ja": "MANUAL（手動対応が必要）",
        "zh_CN": "手动（需要手动干预）",
        "ko": "수동 (수동 개입 필요)",
    },
    "category_datatype": {
        "en": "Data Type Conversion",
        "ja": "データ型変換",
        "zh_CN": "数据类型转换",
        "ko": "데이터 유형 변환",
    },
    "category_function": {
        "en": "Function Conversion",
        "ja": "関数変換",
        "zh_CN": "函数转换",
        "ko": "함수 변환",
    },
    "category_syntax": {
        "en": "Syntax Conversion",
        "ja": "構文変換",
        "zh_CN": "语法转换",
        "ko": "구문 변환",
    },
    "category_plsql": {
        "en": "PL/SQL → PL/pgSQL",
        "ja": "PL/SQL → PL/pgSQL",
        "zh_CN": "PL/SQL → PL/pgSQL",
        "ko": "PL/SQL → PL/pgSQL",
    },
    "category_sequence": {
        "en": "Sequence Conversion",
        "ja": "シーケンス変換",
        "zh_CN": "序列转换",
        "ko": "시퀀스 변환",
    },
    "category_object": {
        "en": "Object Conversion",
        "ja": "オブジェクト変換",
        "zh_CN": "对象转换",
        "ko": "객체 변환",
    },
}


class I18n:
    """Internationalization helper class."""

    def __init__(self, lang_code: str = "en"):
        self.lang_code = lang_code if lang_code in LANGUAGES else "en"

    def t(self, key: str, **kwargs) -> str:
        """Translate a key with optional parameter substitution."""
        translations = TRANSLATIONS.get(key, {})
        text = translations.get(self.lang_code)
        if text is None:
            text = translations.get("en", f"[{key}]")
        if kwargs:
            try:
                text = text.format(**kwargs)
            except (KeyError, IndexError):
                pass
        return text

    def set_language(self, lang_code: str):
        """Change the current language."""
        if lang_code in LANGUAGES:
            self.lang_code = lang_code
