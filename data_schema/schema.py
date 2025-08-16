from pydantic import BaseModel
from typing import Literal



JobDescriptionLabel = Literal["SDR Strategy", "AE Strategy", "Disqualified"]
CompanyDescriptionLabel = Literal["Disqualified", "Qualified"]


class JobQualifier(BaseModel):
    label: JobDescriptionLabel
    # reason: str = Field(..., description="The reason why the particular job description belong to the particular label it was assigned to.")


class CompanyQualifier(BaseModel):
    label: CompanyDescriptionLabel
    # reason: str = Field(..., description="The reason why the particular company belong to the particular label it was assigned to based on the instructions provided.")
