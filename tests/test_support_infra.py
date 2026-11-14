"""测试夹具自身的落点选择：临时根目录必须可写。

``tests/support.py`` 是在**导入期**算 ``BASE_TMP`` 的，它一旦抛 ``OSError``，
整个测试套在收集阶段就死了，报的还是一句跟被测代码毫无关系的错误。这里只钉
"落在哪个目录"这条规则，不碰任何被测行为。

运行::

    python -m unittest tests.test_support_infra -v
"""

from __future__ import annotations

import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

_HERE = Path(__file__).resolve()
_SOURCE_ROOT = _HERE.parents[2]
if str(_SOURCE_ROOT) not in sys.path:
    sys.path.insert(0, str(_SOURCE_ROOT))

from astrbot_plugin_workbuddy.tests import support  # noqa: E402


class TempRootTests(unittest.TestCase):
    def test_the_env_override_wins(self):
        target = os.path.join(tempfile.gettempdir(), "wb-support-env-override")
        with mock.patch.dict(os.environ, {"WORKBUDDY_TEST_TMP": target}):
            self.assertEqual(support._resolve_base_tmp(), Path(target))

    def test_an_unusable_default_falls_back_to_a_writable_dir(self):
        # 故意让 ``parents[3]`` 指向一个**文件**：在它下面建目录必然失败（root 也一样），
        # 正好模拟裸 clone 时 ``/home/tmp`` 那种"建不出来"的情形。
        blocker = Path(tempfile.gettempdir()) / "wb-support-blocker"
        blocker.write_text("", encoding="utf-8")
        self.addCleanup(blocker.unlink, missing_ok=True)
        fake = blocker / "sub" / "pkg" / "tests" / "support.py"
        with mock.patch.object(support, "_HERE", fake):
            base = support._resolve_base_tmp()
        self.assertNotEqual(base, fake.parents[3] / "tmp")
        self.assertTrue(base.is_dir(), "退回去的那个目录也得真的建出来")
        self.assertTrue(os.access(base, os.W_OK))


if __name__ == "__main__":
    unittest.main(verbosity=2)
