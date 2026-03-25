from enum import Enum


class Gender(Enum):
    UNK = 0
    FEMALE = 1
    MALE = 2

    @classmethod
    def from_str(cls, str: str) -> "Gender":
        if str == "m":
            return cls.MALE
        elif str == "f":
            return cls.FEMALE
        return cls.UNK

    def to_str(self) -> str:
        if self == Gender.MALE:
            return "m"
        elif self == Gender.FEMALE:
            return "f"
        return '-'
