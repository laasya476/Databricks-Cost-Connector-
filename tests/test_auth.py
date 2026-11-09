from __future__ import annotations

import unittest
from unittest.mock import patch

from connector import auth


class AuthTests(unittest.TestCase):
    @patch.object(auth.sql, "connect")
    def test_connection_pins_databricks_session_timezone_to_utc(self, connect):
        with patch.dict(
            "os.environ",
            {
                "DATABRICKS_SERVER_HOSTNAME": "workspace.example",
                "DATABRICKS_HTTP_PATH": "/sql/1.0/warehouses/test",
                "DATABRICKS_TOKEN": "test-token",
            },
        ):
            auth.get_connection()

        self.assertEqual(
            connect.call_args.kwargs["session_configuration"],
            {"timezone": "UTC"},
        )


if __name__ == "__main__":
    unittest.main()
