# Databricks notebook source
from __future__ import annotations

from enum import Enum


class StandardTag(str, Enum):
    """Base class for all standard tag enumerations.

    Provides common functionality for tag enums, including a method
    to retrieve the enum member name without its value.

    The KEY class attribute is automatically excluded from enum members
    and can be accessed directly as a string.

    When accessing an enum member directly, it returns its string value.

    Example:
        >>> class MyTag(StandardTag):
        ...     KEY = "my_tag"
        ...     VALUE_ONE = "Value One"
        >>>
        >>> MyTag.KEY
        'my_tag'
        >>> MyTag.VALUE_ONE
        'Value One'
        >>> str(MyTag.VALUE_ONE)
        'Value One'
        >>> MyTag.VALUE_ONE.value
        'Value One'
    """

    def __str__(self) -> str:
        """Return the string value of the enum member.

        Returns:
            The value of the enum member (e.g., 'C2 Internal').
        """
        return self.value

    def __repr__(self) -> str:
        """Return the string value for representation.

        Returns:
            The value of the enum member (e.g., 'C2 Internal').
        """
        return self.value


class ConfidentialityLevel(StandardTag):
    """Confidentiality classification levels for data assets.

    These levels define the sensitivity and access controls for data:
    - C1_PUBLIC: Public data with no restrictions
    - C2_INTERNAL: Internal data restricted to organization members
    - C3_CONFIDENTIAL: Confidential data with strict access controls
    - C4_SECRET: Highly confidential data requiring special authorization
    """

    KEY = "Confidentiality Level"

    C1_PUBLIC = "C1 Public"
    C2_INTERNAL = "C2 Internal"
    C3_CONFIDENTIAL = "C3 Confidential"
    C4_SECRET = "C4 Secret"


class DataDomain(StandardTag):
    """Business domains for data classification.

    High-level categorization of data by business function and purpose.
    """

    KEY = "Data Domain"

    CONTRACT = "Contract"
    COUNTERPARTY = "Counterparty"
    DEAL = "Deal"
    EMPLOYEE = "Employee"
    FINANCIAL_MANAGEMENT = "Financial Management"
    FUNDAMENTAL = "Fundamental"
    MARKET = "Market"
    METERED_AND_VOLUME = "Metered and Volume"
    ORIGINATION = "Origination"
    PHYSICAL_SETTLEMENT = "Physical Settlement"
    PORTFOLIO_MANAGEMENT = "Portfolio Management"
    PROCUREMENT = "Procurement"
    VALUATION_AND_RISK = "Valuation and Risk"


class DataSubdomain(StandardTag):
    """Sub-domains within business domains.

    More specific categorization within each domain.
    Add specific subdomain values as they are defined.
    """

    KEY = "Data Subdomain"

    # Market subdomains
    SPOT = "Spot"
    FORWARD = "Forward"
    INTRADAY = "Intraday"
    BALANCING_AND_RESERVE = "Balancing and Reserve"
    SETTLEMENT = "Settlement"
    MARKET_ANALYTICS_AND_VALUATION = "Market Analytics and Valuation"

    # Fundamental subdomains
    CONSUMPTION = "Consumption"
    TRANSMISSION = "Transmission"
    CONGESTION_MANAGEMENT = "Congestion Management"
    PRODUCTION = "Production"
    DEMAND_AND_SUPPLY = "Demand and Supply"
    HYDRO = "Hydro"
    WEATHER = "Weather"
    LOAD = "Load"
    CONTROLLING = "Controlling"

    # Add other subdomains as needed
    # GENERATION = "Generation"
    # etc.


class LicensedData(StandardTag):
    """Licensed data classification.

    Indicates whether the data is subject to licensing restrictions.
    """

    KEY = "Licensed Data"

    TRUE = "True"
    FALSE = "False"


class PIIClassification(StandardTag):
    """Personal Identifiable Information (PII) classification.

    Indicates whether the data contains personally identifiable
    information and what type:
    - NO_PII: No personal information present
    - LINKED_PII: Contains indirectly identifiable information
    - PII: Contains directly identifiable personal information
    """

    KEY = "Personal Identifiable Information"

    NO_PII = "No PII"
    LINKED_PII = "Linked PII"
    PII = "PII"


class DataOwner(StandardTag):
    """Data owners/teams responsible for data assets.

    Add data owner values as they are identified.
    This enum can be extended as new teams or owners are added.
    """

    KEY = "Data Owner"

    # Individual owners
    CARL_KEVIN_BRUNAES = "Brunaes Carl Kevin"
    CHRISTOPHE_CATTRY = "Cattry Christophe"
    CORSO_QUILICI = "Quilici Corso"
    GIANLUCA_MANCINI = "Mancini Gianluca"
    IRIO_CALASSO = "Calasso Irio"
    JOHANNA_HEIDI_KAUPPINEN = "Kauppinen Heidi Johanna"
    KNUT_STENSROD = "Stensrod Knut"
    LUBOJACKY_VLADISLAV = "Vladislav Lubojacky"
    MICHELE_RIZZI = "Rizzi Michele"
    REMI_JANNER = "Janner Rémi"
    VLADIMIR_SARAMET = "Saramet Vladimir"
    YUFAN_HE = "He Yufan"
    IGNACIO_ORTIZ_DE_ZUNIGA = "Ortiz de Zuniga Ignacio"
    DAVID_PERRAUDIN = "Perraudin David"
    MARIA_LOURDES_SANCHEZ_NUNEZ = "Sanchez Nunez Maria Lourdes"
    YUVER_ALBERTO_TORRES_PENA = "Torres Pena Yuver Alberto"
    JAVIER_HERNANDEZ_MONTES = "Hernandez Montes Javier"
    NORBERT_DORNDORF = "Dorndorf Norbert"
    PER_SIVERSEN = "Siversen Per"
    ZUMSTEG_PATRICK = "Zumsteg Patrick"
    VALENTIN_JULIA = "Julia Valentin"


class StandardTags:
    """Convenience accessor for all standard tag enums.

    Allows access via standard_tags.ConfidentialityLevel.C2_INTERNAL
    """

    ConfidentialityLevel = ConfidentialityLevel
    DataDomain = DataDomain
    DataSubdomain = DataSubdomain
    LicensedData = LicensedData
    PIIClassification = PIIClassification
    DataOwner = DataOwner


# Singleton instance for convenient access
standard_tags = StandardTags()


# Example usage:
# Access KEY directly as string:
#   standard_tags.ConfidentialityLevel.KEY -> "confidentiality_level"
#
# Access enum member returns the string value:
#   standard_tags.ConfidentialityLevel.C1_PUBLIC -> "C1 Public"
#   str(standard_tags.ConfidentialityLevel.C1_PUBLIC) -> "C1 Public"
#
# Access via .value property (explicit):
#   standard_tags.ConfidentialityLevel.C1_PUBLIC.value -> "C1 Public"
