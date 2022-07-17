import logging
import typing as tp

from airflow.models.baseoperator import BaseOperator
from airflow.providers.http.hooks.http import HttpHook


class TValievRamHook(HttpHook):
    """Interacts with Rick&Morty API"""

    def __init__(self, http_conn_id: str, **kwargs) -> None:
        super().__init__(http_conn_id=http_conn_id, **kwargs)
        self.method = "GET"

    def get_page_count(self, section: str = None) -> int:
        """return count of pages in API section.

        Args:
            section (str, optional): API section (character, location, etc.). Defaults to None.

        Returns:
            int: pages number.
        """
        pages = int(self.run(f"api/{section}").json()["info"]["pages"])
        logging.info(f"pages num: {pages}")
        return pages

    def get_section_results(
        self, page_num: tp.Union[str, int], section: str = None
    ) -> list:
        """return page results

        Args:
            page_num (Union[str, int]): page number
            section (str, optional): API section (character, location, etc.). Defaults to None.

        Returns:
            list: page results
        """

        return self.run(f"api/{section}?page={page_num}").json()["results"]


class TValievRamSectionOperator(BaseOperator):
    """Count first results by key on TValievRamHook

    Args:
        conn_id (str): connection id.
        section (str, optional): API section (character, location, etc.). Defaults to None.
        n (int): return first n values. Defaults to None.
        key (callable, optional): compare function for field. Defaults to None.
        reverse (bool, optional): return last elements. Defaults to False.
    """

    template_fields = ("section", "reverse")
    ui_color = "#4ca64e"

    def __init__(
        self,
        conn_id: str,
        section: str,
        n: int = None,
        key: tp.Callable = None,
        reverse: bool = False,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.conn_id = conn_id
        self.section = section
        self.key = key
        self.reverse = reverse
        self.n = n


    def execute(self, context):
        ram_hook = TValievRamHook(http_conn_id=self.conn_id)
        res_pairs = []

        
        for page_num in range(ram_hook.get_page_count(self.section)):
            page_res = ram_hook.get_section_results(
                page_num=page_num + 1, section=self.section
            )

            res_pairs.extend(page_res)

        res_pairs.sort(key=self.key, reverse=self.reverse)

        if self.n is None:
            return res_pairs
        else:
            return res_pairs[:self.n]
