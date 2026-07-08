"""Tests de humo de la CLI de demostración (`__main__.py`)."""

from __future__ import annotations

from pathlib import Path

import pytest

from write_ahead_log_recovery.__main__ import main


def test_end_to_end_insert_commit_and_recover(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    wal_path = tmp_path / "wal.log"

    assert main(["begin", "--path", str(wal_path)]) == 0
    txn_id = int(capsys.readouterr().out.strip())

    assert (
        main(
            [
                "insert",
                "--path",
                str(wal_path),
                "--txn",
                str(txn_id),
                "--table",
                "users",
                "--key",
                "alice",
                "--value",
                "v1",
            ]
        )
        == 0
    )
    capsys.readouterr()

    assert main(["commit", "--path", str(wal_path), "--txn", str(txn_id)]) == 0
    capsys.readouterr()

    assert main(["dump", "--path", str(wal_path)]) == 0
    dump_output = capsys.readouterr().out
    assert "type=INSERT" in dump_output
    assert "type=COMMIT" in dump_output

    assert main(["recover", "--path", str(wal_path)]) == 0
    recover_output = capsys.readouterr().out
    assert "redone=1" in recover_output
    assert "state: users[b'alice'] = b'v1'" in recover_output


def test_checkpoint_command_reports_truncation(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    wal_path = tmp_path / "wal.log"
    main(["begin", "--path", str(wal_path)])
    txn_id = int(capsys.readouterr().out.strip())
    main(
        [
            "insert",
            "--path",
            str(wal_path),
            "--txn",
            str(txn_id),
            "--table",
            "users",
            "--key",
            "alice",
            "--value",
            "v1",
        ]
    )
    main(["commit", "--path", str(wal_path), "--txn", str(txn_id)])
    capsys.readouterr()

    assert main(["checkpoint", "--path", str(wal_path), "--active", "", "--truncate"]) == 0
    output = capsys.readouterr().out
    assert "begin_lsn=" in output
    assert "discarded=" in output


def test_unknown_command_exits_with_argparse_error() -> None:
    with pytest.raises(SystemExit):
        main(["not-a-real-command", "--path", "whatever"])
