from dataclasses import dataclass, field
from typing import Any

import requests

from data_platform.etl.core import TaskContext
from data_platform.etl.etl_task import ETLTask


@dataclass
class SendTeamsAlertTask(ETLTask):
    """A task that sends formatted alert messages to a Microsoft Teams channel via webhook.

    This task constructs a MessageCard payload using the provided alert data and sends it
    to the specified Teams webhook URL. It is typically used to notify users of validation
    results or other ETL-related events.

    Attributes:
        teams_webhook_url (str): The Microsoft Teams webhook URL to send alerts to.
        title (str): The title of the alert message card.
        message_sections_list (List[Dict[str, Any]]): A list of message sections, each containing
            'activityTitle', 'facts', and 'markdown' keys. Defaults to an empty list.
        description (str): A description shown in the message card. Defaults to a standard message.
        context (TaskContext): The ETL task context, including logger and metadata.
        task_name (str): The name of the task. Defaults to "SendTeamsAlertTask".
    """

    context: TaskContext
    teams_webhook_url: str
    application_title: str
    message_sections_list: list[dict[str, Any]] = field(default_factory=list)
    description: str = "The following validations were executed:"
    theme_color: str = "0078D7"
    task_name = "SendTeamsAlertTask"

    def __post_init__(self) -> None:
        """Post-initialization hook to validate message sections if any are provided.

        Ensures that each section adheres to the expected schema.
        """
        if self.message_sections_list:  # Only validate if non-empty
            for idx, message in enumerate(self.message_sections_list):
                self._validate_message_sections(message, idx)

    def _validate_message_sections(self, message: dict[str, Any], idx: int) -> None:
        """Validates the structure of a single message section.

        Parameters:
            message (Dict[str, Any]): The message section to validate.
            idx (int): The index of the message in the list for error reporting.

        Raises:
            ValueError: If the message does not conform to the expected schema.
        """
        if not isinstance(message, dict):
            raise ValueError(f"Message at index {idx} is not a dictionary")

        required_keys = {"activityTitle", "facts", "markdown"}
        if not required_keys.issubset(message.keys()):
            raise ValueError(f"Message at index {idx} is missing required keys")

        if not isinstance(message["activityTitle"], str):
            raise ValueError(f"Message at index {idx} has an invalid 'activityTitle'")

        if not isinstance(message["facts"], list):
            raise ValueError(f"Message at index {idx} has an invalid 'facts'")

        for fact in message["facts"]:
            if not isinstance(fact, dict) or "name" not in fact or "value" not in fact:
                raise ValueError(f"Message at index {idx} has an invalid 'facts' entry")

        if not isinstance(message["markdown"], bool):
            raise ValueError(f"Message at index {idx} has an invalid 'markdown'")

    def execute(self) -> None:
        """Executes the alert task by formatting the message card and sending it to Teams.

        This method logs the execution process, builds the message payload, and
        sends it to the configured webhook URL.
        """
        self.context.logger.info(f"Executing {self.task_name}")

        # Format the message card payload
        json_payload = self._format_message_card()

        # Send the payload to Microsoft Teams
        self._send_teams_alert(json_payload)

    def _format_message_card(self) -> dict:
        """Constructs the MessageCard JSON payload for Microsoft Teams.

        Returns:
            dict: A dictionary representing the MessageCard structure.
        """
        return {
            "@type": "MessageCard",
            "@context": "http://schema.org/extensions",
            "themeColor": self.theme_color,
            "title": self.application_title,
            "text": self.description,
            "sections": self.message_sections_list,
            "potentialAction": [],  # Placeholder for future actions (e.g., buttons)
        }

    def _send_teams_alert(self, json_payload: dict) -> None:
        """Sends the formatted MessageCard payload to the Microsoft Teams webhook.

        Parameters:
            json_payload (dict): The JSON payload to send.

        Raises:
            requests.RequestException: If the HTTP request fails.
        """
        self.context.logger.info("Sending alerts to the configured URL")
        self.context.logger.info(f"Alerts URL: {self.teams_webhook_url}")
        self.context.logger.info(f"JSON Payload: {json_payload}")

        response = requests.post(self.teams_webhook_url, json=json_payload, timeout=10)
        try:
            response.raise_for_status()
            self.context.logger.info("Alerts sent successfully")
        except requests.RequestException as err:
            self.context.logger.error(f"Failed to send alerts: {err}, response: {getattr(response, 'text', None)}")
            raise
