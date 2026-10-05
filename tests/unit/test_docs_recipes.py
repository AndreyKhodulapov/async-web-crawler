"""Unit tests: the recipes of the configuration guide are configurations the crawler accepts."""

import re
from pathlib import Path

import pytest

from crawler import load_config

GUIDE = Path(__file__).parents[2] / "docs" / "configuration.md"
YAML_BLOCK = re.compile(r"^```yaml\n(.*?)^```", re.MULTILINE | re.DOTALL)
SECTION = re.compile(r"^(\w+):", re.MULTILINE)


def recipes() -> list[str]:
    """The YAML blocks of the "Recipes" section, the last one of the guide."""
    text = GUIDE.read_text(encoding="utf-8")
    _, found, section = text.partition("\n## Recipes\n")
    assert found, "the guide has no Recipes section"
    assert "\n## " not in section, "the Recipes section is no longer the last one"
    return YAML_BLOCK.findall(section)


def name(recipe: str) -> str:
    """A recipe by its sections: `crawler+rendering`."""
    return "+".join(section for section in SECTION.findall(recipe) if section != "urls")


def test_the_recipes_are_found():
    assert len(recipes()) >= 7


@pytest.mark.parametrize("recipe", recipes(), ids=name)
def test_a_recipe_is_a_valid_configuration(recipe, tmp_path):
    path = tmp_path / "config.yaml"
    path.write_text(recipe, encoding="utf-8")

    config = load_config(path)  # raises ConfigError with every problem

    assert config.urls
