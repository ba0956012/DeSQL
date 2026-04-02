"""
把 EVALUATION_REPORT.md 轉成獨立 HTML（圖片嵌入 base64）
用法：python eval/generate_html.py
"""

import re
import base64
from pathlib import Path

REPORT_DIR = Path(__file__).parent / "report"
MD_PATH = REPORT_DIR / "EVALUATION_REPORT.md"
HTML_PATH = REPORT_DIR / "EVALUATION_REPORT.html"


def img_to_base64(img_path: Path) -> str:
    with open(img_path, "rb") as f:
        return base64.b64encode(f.read()).decode()


def md_to_html(md_text: str) -> str:
    lines = md_text.split("\n")
    html_lines = []
    in_table = False
    in_code = False

    for line in lines:
        stripped = line.strip()

        # code block
        if stripped.startswith("```"):
            if in_code:
                html_lines.append("</code></pre>")
                in_code = False
            else:
                html_lines.append("<pre><code>")
                in_code = True
            continue
        if in_code:
            html_lines.append(line)
            continue

        # image
        img_match = re.match(r"!\[(.+?)\]\((.+?)\)", stripped)
        if img_match:
            alt, src = img_match.groups()
            img_path = REPORT_DIR / src
            if img_path.exists():
                b64 = img_to_base64(img_path)
                html_lines.append(
                    f'<img src="data:image/png;base64,{b64}" alt="{alt}" style="max-width:100%;">'
                )
            else:
                html_lines.append(f'<p>[Image not found: {src}]</p>')
            continue

        # heading
        if stripped.startswith("# "):
            html_lines.append(f"<h1>{stripped[2:]}</h1>")
            continue
        if stripped.startswith("## "):
            html_lines.append(f"<h2>{stripped[3:]}</h2>")
            continue
        if stripped.startswith("### "):
            html_lines.append(f"<h3>{stripped[4:]}</h3>")
            continue

        # table
        if "|" in stripped and stripped.startswith("|"):
            cells = [c.strip() for c in stripped.split("|")[1:-1]]
            if all(set(c) <= set("- :") for c in cells):
                continue  # separator row
            if not in_table:
                html_lines.append("<table>")
                in_table = True
                html_lines.append("<tr>" + "".join(f"<th>{c}</th>" for c in cells) + "</tr>")
            else:
                # bold cell
                cells_html = []
                for c in cells:
                    if c.startswith("**") and c.endswith("**"):
                        cells_html.append(f"<td><strong>{c[2:-2]}</strong></td>")
                    else:
                        cells_html.append(f"<td>{c}</td>")
                html_lines.append("<tr>" + "".join(cells_html) + "</tr>")
            continue
        else:
            if in_table:
                html_lines.append("</table>")
                in_table = False

        # list
        if stripped.startswith("- "):
            html_lines.append(f"<li>{stripped[2:]}</li>")
            continue
        list_match = re.match(r"(\d+)\. (.+)", stripped)
        if list_match:
            html_lines.append(f"<li>{list_match.group(2)}</li>")
            continue

        # empty line
        if not stripped:
            html_lines.append("")
            continue

        # paragraph
        html_lines.append(f"<p>{stripped}</p>")

    if in_table:
        html_lines.append("</table>")

    return "\n".join(html_lines)


def main():
    md_text = MD_PATH.read_text(encoding="utf-8")
    body = md_to_html(md_text)

    html = f"""<!DOCTYPE html>
<html lang="zh-TW">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>DeSQL Evaluation Report</title>
<style>
  body {{ font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Helvetica, Arial, sans-serif; max-width: 900px; margin: 40px auto; padding: 0 20px; color: #24292e; line-height: 1.6; }}
  h1 {{ border-bottom: 2px solid #4472C4; padding-bottom: 8px; }}
  h2 {{ border-bottom: 1px solid #eee; padding-bottom: 6px; margin-top: 32px; }}
  h3 {{ margin-top: 24px; }}
  table {{ border-collapse: collapse; width: 100%; margin: 16px 0; }}
  th, td {{ border: 1px solid #ddd; padding: 8px 12px; text-align: left; }}
  th {{ background: #4472C4; color: white; }}
  tr:nth-child(even) {{ background: #f6f8fa; }}
  img {{ margin: 16px 0; border: 1px solid #eee; border-radius: 4px; }}
  pre {{ background: #f6f8fa; padding: 12px; border-radius: 4px; overflow-x: auto; }}
  li {{ margin: 4px 0; }}
  strong {{ color: #2E75B6; }}
</style>
</head>
<body>
{body}
</body>
</html>"""

    HTML_PATH.write_text(html, encoding="utf-8")
    print(f"✅ {HTML_PATH}")


if __name__ == "__main__":
    main()
