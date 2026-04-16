from __future__ import annotations

import os

from django.core.exceptions import ImproperlyConfigured
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build


GOOGLE_BUSINESS_MANAGE_SCOPE = "https://www.googleapis.com/auth/business.manage"


class GoogleBusinessProfileClient:
    def __init__(self):
        client_id = str(os.getenv("GOOGLE_BUSINESS_PROFILE_CLIENT_ID") or "").strip()
        client_secret = str(os.getenv("GOOGLE_BUSINESS_PROFILE_CLIENT_SECRET") or "").strip()
        refresh_token = str(os.getenv("GOOGLE_BUSINESS_PROFILE_REFRESH_TOKEN") or "").strip()
        token_uri = str(os.getenv("GOOGLE_BUSINESS_PROFILE_TOKEN_URI") or "https://oauth2.googleapis.com/token").strip()
        if not client_id or not client_secret or not refresh_token:
            raise ImproperlyConfigured(
                "Faltan credenciales OAuth de Google Business Profile: "
                "GOOGLE_BUSINESS_PROFILE_CLIENT_ID, GOOGLE_BUSINESS_PROFILE_CLIENT_SECRET y GOOGLE_BUSINESS_PROFILE_REFRESH_TOKEN."
            )
        self._credentials = Credentials(
            token=None,
            refresh_token=refresh_token,
            token_uri=token_uri,
            client_id=client_id,
            client_secret=client_secret,
            scopes=[GOOGLE_BUSINESS_MANAGE_SCOPE],
        )
        self._service = build(
            "mybusinessbusinessinformation",
            "v1",
            credentials=self._credentials,
            cache_discovery=False,
        )

    def get_location(self, *, location_name: str, read_mask: str = "regularHours,specialHours,metadata") -> dict:
        return self._service.locations().get(name=location_name, readMask=read_mask).execute()

    def patch_special_hours(self, *, location_name: str, special_hour_periods: list[dict], validate_only: bool = False) -> dict:
        body = {
            "name": location_name,
            "specialHours": {
                "specialHourPeriods": special_hour_periods,
            },
        }
        return (
            self._service.locations()
            .patch(
                name=location_name,
                updateMask="specialHours",
                validateOnly=validate_only,
                body=body,
            )
            .execute()
        )

