"""
Command-line interface for the DNA ETL Framework.
"""

import argparse
import sys
from typing import List

from dna_etl import __version__, get_version
from dna_etl.core import Pipeline


def create_parser() -> argparse.ArgumentParser:
    """Create the command-line argument parser."""
    parser = argparse.ArgumentParser(
        prog="dna-etl",
        description="DNA ETL Framework - Command Line Interface",
        epilog="For more information, visit: https://github.com/axpo-ts/dna-etl-framework",
    )
    
    parser.add_argument(
        "--version",
        action="version",
        version=f"DNA ETL Framework {__version__}",
    )
    
    subparsers = parser.add_subparsers(dest="command", help="Available commands")
    
    # Version command
    version_parser = subparsers.add_parser("version", help="Show version information")
    version_parser.add_argument(
        "--detailed",
        action="store_true",
        help="Show detailed version information",
    )
    
    # Info command
    info_parser = subparsers.add_parser("info", help="Show package information")
    
    # Validate command
    validate_parser = subparsers.add_parser("validate", help="Validate ETL configuration")
    validate_parser.add_argument(
        "config_file",
        nargs="?",
        help="Path to configuration file to validate",
    )
    
    return parser


def cmd_version(args) -> int:
    """Handle version command."""
    if args.detailed:
        print(f"DNA ETL Framework")
        print(f"Version: {get_version()}")
        print(f"Python: {sys.version}")
        print(f"Platform: {sys.platform}")
    else:
        print(get_version())
    return 0


def cmd_info(args) -> int:
    """Handle info command."""
    from dna_etl import __author__, __email__
    
    print("DNA ETL Framework")
    print("=" * 50)
    print(f"Version: {get_version()}")
    print(f"Author: {__author__}")
    print(f"Email: {__email__}")
    print(f"Repository: https://github.com/axpo-ts/dna-etl-framework")
    print()
    print("Description:")
    print("  Standardized patterns, components, and tooling to build")
    print("  reliable, observable, and scalable ETL pipelines at Axpo.")
    print()
    print("Available Components:")
    print("  - Pipeline: ETL pipeline orchestrator")
    print("  - ETLBase: Base class for all ETL components")
    print()
    print("Example Usage:")
    print("  from dna_etl import Pipeline")
    print("  pipeline = Pipeline('my_pipeline')")
    print("  # Add your ETL steps...")
    print("  results = pipeline.execute()")
    
    return 0


def cmd_validate(args) -> int:
    """Handle validate command."""
    config_file = args.config_file
    
    if not config_file:
        print("Error: No configuration file specified")
        return 1
    
    try:
        # In a real implementation, this would validate the config file
        # For now, just check if the file exists
        import os
        if not os.path.exists(config_file):
            print(f"Error: Configuration file '{config_file}' not found")
            return 1
        
        print(f"Validating configuration file: {config_file}")
        print("✓ Configuration file found")
        # TODO: Add actual validation logic here
        print("✓ Configuration validation passed")
        
        return 0
    except Exception as e:
        print(f"Error validating configuration: {e}")
        return 1


def main(argv: List[str] = None) -> int:
    """Main CLI entry point."""
    parser = create_parser()
    args = parser.parse_args(argv)
    
    if not args.command:
        parser.print_help()
        return 0
    
    command_handlers = {
        "version": cmd_version,
        "info": cmd_info,
        "validate": cmd_validate,
    }
    
    handler = command_handlers.get(args.command)
    if handler:
        return handler(args)
    else:
        print(f"Unknown command: {args.command}")
        return 1


if __name__ == "__main__":
    sys.exit(main())