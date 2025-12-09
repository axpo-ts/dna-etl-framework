%md

# SendTeamsAlertTask

## Overview

`SendTeamsAlertTask` is an ETL task designed to send alert notifications to a Microsoft Teams channel using a webhook. It formats the alert data into a **MessageCard** payload and posts it to Teams.

---

## Attributes

| **Attribute**           | **Type**               | **Description**                                                               |
| ----------------------- | ---------------------- | ----------------------------------------------------------------------------- |
| `context`               | `TaskContext`          | Provides logging and metadata for the task execution.                         |
| `teams_webhook_url`     | `str`                  | The Microsoft Teams webhook URL to send alerts to.                            |
| `application_title`     | `str`                  | Title displayed at the top of the message card.                               |
| `message_sections_list` | `list[dict[str, Any]]` | List of message sections, each with `activityTitle`, `facts`, and `markdown`. |
| `description`           | `str`                  | Description shown in the card body. Defaults to a standard message.           |
| `theme_color`           | `str`                  | Hex color code for the card accent. Defaults to Microsoft blue (`0078D7`).    |
| `task_name`             | `str`                  | Name of the task. Defaults to `"SendTeamsAlertTask"`.                         |

## MessageCard Format

The payload sent to Teams follows the [MessageCard schema](https://learn.microsoft.com/en-us/outlook/actionable-messages/message-card-reference). Each alert is represented as a section with the following fields:

| **Field**       | **Type**               | **Description**                                                              |
| --------------- | ---------------------- | ---------------------------------------------------------------------------- |
| `activityTitle` | `str`                  | Title of the alert section, typically includes the alert name.               |
| `facts`         | `list[dict[str, str]]` | Key-value pairs describing alert details (e.g., description, count, sample). |
| `markdown`      | `bool`                 | Enables markdown formatting within the section for better readability.       |

## Methods

| **Method**                                 | **Description**                                                                                                                       |
| ------------------------------------------ | ------------------------------------------------------------------------------------------------------------------------------------- |
| `__post_init__()`                          | Validates each section in `message_sections_list` to ensure it conforms to the expected schema.                                       |
| `_validate_message_sections(message, idx)` | Checks that each message section is a valid dictionary with required keys and correct types. Raises `ValueError` if validation fails. |
| `execute()`                                | Main entry point for the task. Logs execution, builds the message card payload, and sends it to Teams.                                |
| `_format_message_card()` → `dict`          | Constructs the JSON payload for the MessageCard, including title, description, sections, and theme color.                             |
| `_send_teams_alert(json_payload)`          | Sends the formatted payload to the Teams webhook URL using `requests.post`. Logs success or failure.                                  |

## Example

from data_platform.etl.alerts.send_teams_alert import SendTeamsAlertTask

from data_platform.etl.core.task_context import TaskContext

#### Microsoft Teams webhook URL to send alerts to

teams_webhook_url = "<Add webhook url>"

####Title displayed at the top of the message card
application_title = "CPU Monitoring"

####List of message sections, each with activityTitle, facts, and markdown
sections_data =

```json
[
  {
    "activityTitle": "Alert 1: HighCPU_app01",
    "facts": [
      {
        "name": "Description",
        "value": "CPU usage exceeded"
      },
      {
        "name": "Alert Records Count:",
        "value": 42
      },
      {
        "name": "Sample Alert Records",
        "value": "[{'id': 1, 'cpu': 95}]"
      }
    ],
    "markdown": true
  },
  {
    "activityTitle": "Alert 2: Memory_app02",
    "facts": [
      {
        "name": "Description",
        "value": "Memory usage exceeded"
      },
      {
        "name": "Alert Records Count:",
        "value": 17
      },
      {
        "name": "Sample Alert Records",
        "value": "[{'id': 2, 'mem': 88}]"
      }
    ],
    "markdown": true
  }
]


####Create an instance of SendTeamsAlertTask and execute it
SendTeamsAlertTask(
    context=TaskContext(),
    teams_webhook_url=teams_webhook_url,
    application_title=application_title,
    message_sections_list=sections_data
).execute()
```
