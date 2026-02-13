"""
Zendesk API client module.
Handles all API communication with Zendesk.
"""

import threading
import time
from typing import Optional, List, Dict, Any

import requests
from requests.auth import HTTPBasicAuth

from zendesk_dc_manager.config import (
    API_CONFIG,
    logger,
)


class ZendeskAPI:
    """Zendesk API client with rate limiting and retry logic."""

    def __init__(self, subdomain: str, email: str, token: str):
        self.subdomain = subdomain
        self.base_url = f"https://{subdomain}.zendesk.com/api/v2"
        self.hc_base_url = f"https://{subdomain}.zendesk.com/api/v2/help_center"
        self.auth = HTTPBasicAuth(f"{email}/token", token)
        self.session = requests.Session()
        self.session.auth = self.auth
        self.session.headers.update({
            'Content-Type': 'application/json',
            'Accept': 'application/json'
        })

        self._stop_flag = False
        self._stop_lock = threading.Lock()
        self._last_request_time = 0
        self._request_lock = threading.Lock()

    def stop(self):
        """Signal to stop current operation."""
        with self._stop_lock:
            self._stop_flag = True

    def reset_stop(self):
        """Reset stop flag."""
        with self._stop_lock:
            self._stop_flag = False

    def _should_stop(self) -> bool:
        """Check if operation should stop."""
        with self._stop_lock:
            return self._stop_flag

    def _rate_limit(self):
        """Enforce rate limiting between requests."""
        with self._request_lock:
            now = time.time()
            min_interval = 0.1
            elapsed = now - self._last_request_time
            if elapsed < min_interval:
                time.sleep(min_interval - elapsed)
            self._last_request_time = time.time()

    def _request(
        self,
        method: str,
        url: str,
        data: Dict = None,
        params: Dict = None,
        retries: int = None,
        retry_on_404: bool = True
    ) -> requests.Response:
        """Make an API request with retry logic."""
        if self._should_stop():
            raise Exception("Operation canceled")

        if retries is None:
            retries = API_CONFIG.RETRY_COUNT

        self._rate_limit()

        for attempt in range(retries + 1):
            if self._should_stop():
                raise Exception("Operation canceled")

            try:
                if method.upper() == 'GET':
                    response = self.session.get(
                        url,
                        params=params,
                        timeout=API_CONFIG.TIMEOUT_DEFAULT
                    )
                elif method.upper() == 'POST':
                    response = self.session.post(
                        url,
                        json=data,
                        timeout=API_CONFIG.TIMEOUT_DEFAULT
                    )
                elif method.upper() == 'PUT':
                    response = self.session.put(
                        url,
                        json=data,
                        timeout=API_CONFIG.TIMEOUT_DEFAULT
                    )
                elif method.upper() == 'DELETE':
                    response = self.session.delete(
                        url,
                        timeout=API_CONFIG.TIMEOUT_DEFAULT
                    )
                else:
                    raise ValueError(f"Unknown method: {method}")

                if response.status_code == 429:
                    retry_after = int(
                        response.headers.get(
                            'Retry-After',
                            API_CONFIG.RATE_LIMIT_MAX_WAIT
                        )
                    )
                    logger.warning(
                        f"Rate limited. Waiting {retry_after} seconds..."
                    )
                    time.sleep(retry_after)
                    continue

                # Don't retry 404 errors unless explicitly requested
                if response.status_code == 404 and not retry_on_404:
                    response.raise_for_status()

                response.raise_for_status()
                return response

            except requests.exceptions.RequestException as e:
                # Don't retry 404 errors - resource doesn't exist
                if '404' in str(e) and not retry_on_404:
                    raise

                if attempt < retries:
                    wait_time = API_CONFIG.RETRY_BASE_DELAY * (attempt + 1)
                    logger.warning(
                        f"Request failed (attempt {attempt + 1}): {e}. "
                        f"Retrying in {wait_time}s..."
                    )
                    time.sleep(wait_time)
                else:
                    raise

        raise Exception(f"Request failed after {retries} retries")

    def _paginate(
        self,
        url: str,
        key: str,
        params: Dict = None,
        retry_on_404: bool = True
    ) -> List[Dict]:
        """Paginate through API results."""
        results = []
        params = params or {}
        page_count = 0

        while url and page_count < API_CONFIG.MAX_PAGINATION_PAGES:
            if self._should_stop():
                raise Exception("Operation canceled")

            response = self._request(
                'GET', url, params=params, retry_on_404=retry_on_404
            )
            data = response.json()

            if key in data:
                results.extend(data[key])
            elif isinstance(data, list):
                results.extend(data)

            url = data.get('next_page')
            params = {}
            page_count += 1

        return results

    # =========================================================================
    # USER / ACCOUNT
    # =========================================================================

    def get_current_user(self) -> Dict[str, Any]:
        """Get current authenticated user info."""
        url = f"{self.base_url}/users/me"
        response = self._request('GET', url)
        return response.json().get('user', {})

    def get_default_locale(self) -> Dict[str, Any]:
        """Get the default locale for the account."""
        url = f"{self.base_url}/locales/default"
        response = self._request('GET', url)
        return response.json().get('locale', {})

    # =========================================================================
    # DYNAMIC CONTENT
    # =========================================================================

    def get_dynamic_content_items(self) -> List[Dict]:
        """Get all dynamic content items."""
        url = f"{self.base_url}/dynamic_content/items"
        return self._paginate(url, 'items')

    def get_dynamic_content_item(self, dc_id: int) -> Dict[str, Any]:
        """Get a single dynamic content item."""
        url = f"{self.base_url}/dynamic_content/items/{dc_id}"
        response = self._request('GET', url)
        return response.json().get('item', {})

    def create_dynamic_content(
        self,
        name: str,
        default_locale_id: int,
        variants: List[Dict]
    ) -> Dict[str, Any]:
        """Create a new dynamic content item."""
        url = f"{self.base_url}/dynamic_content/items"
        data = {
            'item': {
                'name': name,
                'default_locale_id': default_locale_id,
                'variants': variants
            }
        }
        response = self._request('POST', url, data=data)
        return response.json().get('item', {})

    def update_dynamic_content_variants(
        self,
        dc_id: int,
        variants: List[Dict]
    ) -> Dict[str, Any]:
        """Update variants of a dynamic content item."""
        dc_item = self.get_dynamic_content_item(dc_id)
        existing_variants = {
            v['locale_id']: v for v in dc_item.get('variants', [])
        }

        for variant in variants:
            locale_id = variant['locale_id']
            content = variant['content']

            if locale_id in existing_variants:
                variant_id = existing_variants[locale_id]['id']
                url = (
                    f"{self.base_url}/dynamic_content/items/{dc_id}"
                    f"/variants/{variant_id}"
                )
                self._request('PUT', url, data={
                    'variant': {'content': content}
                })
            else:
                url = f"{self.base_url}/dynamic_content/items/{dc_id}/variants"
                self._request('POST', url, data={
                    'variant': {
                        'locale_id': locale_id,
                        'content': content
                    }
                })

        return self.get_dynamic_content_item(dc_id)

    # =========================================================================
    # TICKET FIELDS
    # =========================================================================

    def get_ticket_fields(self) -> List[Dict]:
        """Get all ticket fields."""
        url = f"{self.base_url}/ticket_fields"
        return self._paginate(url, 'ticket_fields')

    def get_ticket_field(self, field_id: int) -> Dict[str, Any]:
        """Get a single ticket field with its options."""
        url = f"{self.base_url}/ticket_fields/{field_id}"
        response = self._request('GET', url)
        return response.json().get('ticket_field', {})

    def update_ticket_field(
        self,
        field_id: int,
        data: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Update a ticket field."""
        url = f"{self.base_url}/ticket_fields/{field_id}"
        response = self._request('PUT', url, data={'ticket_field': data})
        return response.json().get('ticket_field', {})

    def get_ticket_field_options(self, field_id: int) -> List[Dict]:
        """Get fresh options for a ticket field."""
        field = self.get_ticket_field(field_id)
        return field.get('custom_field_options', [])

    def update_ticket_field_option_via_field(
        self,
        field_id: int,
        option_value: str,
        new_name: str
    ) -> Dict[str, Any]:
        """
        Update a ticket field option by updating the entire field.
        This is more reliable than updating individual options.
        """
        # Get current field with all options
        field = self.get_ticket_field(field_id)
        options = field.get('custom_field_options', [])

        if not options:
            raise Exception(f"Field {field_id} has no options")

        # Find and update the target option
        found = False
        updated_options = []
        for opt in options:
            if opt.get('value') == option_value:
                # Update this option's name
                updated_options.append({
                    'name': new_name,
                    'value': opt.get('value')
                })
                found = True
                logger.debug(
                    f"Updating option value={option_value} to name={new_name}"
                )
            else:
                # Keep existing option
                updated_options.append({
                    'name': opt.get('raw_name', opt.get('name', '')),
                    'value': opt.get('value')
                })

        if not found:
            raise Exception(
                f"Option with value '{option_value}' not found in "
                f"field {field_id}"
            )

        # Update the field with modified options
        return self.update_ticket_field(
            field_id,
            {'custom_field_options': updated_options}
        )

    def update_ticket_field_option(
        self,
        field_id: int,
        option_id: int,
        data: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Update a ticket field option (legacy method)."""
        url = f"{self.base_url}/ticket_fields/{field_id}/options/{option_id}"
        response = self._request('PUT', url, data={
            'custom_field_option': data
        })
        return response.json().get('custom_field_option', {})

    # =========================================================================
    # TICKET FORMS
    # =========================================================================

    def get_ticket_forms(self) -> List[Dict]:
        """Get all ticket forms."""
        url = f"{self.base_url}/ticket_forms"
        return self._paginate(url, 'ticket_forms')

    def update_ticket_form(
        self,
        form_id: int,
        data: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Update a ticket form."""
        url = f"{self.base_url}/ticket_forms/{form_id}"
        response = self._request('PUT', url, data={'ticket_form': data})
        return response.json().get('ticket_form', {})

    # =========================================================================
    # CUSTOM STATUSES
    # =========================================================================

    def get_custom_statuses(self) -> List[Dict]:
        """Get all custom ticket statuses."""
        url = f"{self.base_url}/custom_statuses"
        response = self._request('GET', url)
        return response.json().get('custom_statuses', [])

    def update_custom_status(
        self,
        status_id: int,
        data: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Update a custom status."""
        url = f"{self.base_url}/custom_statuses/{status_id}"
        response = self._request('PUT', url, data={'custom_status': data})
        return response.json().get('custom_status', {})

    # =========================================================================
    # USER FIELDS
    # =========================================================================

    def get_user_fields(self) -> List[Dict]:
        """Get all user fields."""
        url = f"{self.base_url}/user_fields"
        return self._paginate(url, 'user_fields')

    def get_user_field(self, field_id: int) -> Dict[str, Any]:
        """Get a single user field with its options."""
        url = f"{self.base_url}/user_fields/{field_id}"
        response = self._request('GET', url)
        return response.json().get('user_field', {})

    def update_user_field(
        self,
        field_id: int,
        data: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Update a user field."""
        url = f"{self.base_url}/user_fields/{field_id}"
        response = self._request('PUT', url, data={'user_field': data})
        return response.json().get('user_field', {})

    def get_user_field_options(self, field_id: int) -> List[Dict]:
        """Get fresh options for a user field."""
        field = self.get_user_field(field_id)
        return field.get('custom_field_options', [])

    def update_user_field_option_via_field(
        self,
        field_id: int,
        option_value: str,
        new_name: str
    ) -> Dict[str, Any]:
        """Update a user field option by updating the entire field."""
        field = self.get_user_field(field_id)
        options = field.get('custom_field_options', [])

        if not options:
            raise Exception(f"User field {field_id} has no options")

        found = False
        updated_options = []
        for opt in options:
            if opt.get('value') == option_value:
                updated_options.append({
                    'name': new_name,
                    'value': opt.get('value')
                })
                found = True
            else:
                updated_options.append({
                    'name': opt.get('raw_name', opt.get('name', '')),
                    'value': opt.get('value')
                })

        if not found:
            raise Exception(
                f"Option with value '{option_value}' not found in "
                f"user field {field_id}"
            )

        return self.update_user_field(
            field_id,
            {'custom_field_options': updated_options}
        )

    def update_user_field_option(
        self,
        field_id: int,
        option_id: int,
        data: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Update a user field option (legacy method)."""
        url = f"{self.base_url}/user_fields/{field_id}/options/{option_id}"
        response = self._request('PUT', url, data={
            'custom_field_option': data
        })
        return response.json().get('custom_field_option', {})

    # =========================================================================
    # ORGANIZATION FIELDS
    # =========================================================================

    def get_organization_fields(self) -> List[Dict]:
        """Get all organization fields."""
        url = f"{self.base_url}/organization_fields"
        return self._paginate(url, 'organization_fields')

    def get_organization_field(self, field_id: int) -> Dict[str, Any]:
        """Get a single organization field with its options."""
        url = f"{self.base_url}/organization_fields/{field_id}"
        response = self._request('GET', url)
        return response.json().get('organization_field', {})

    def update_organization_field(
        self,
        field_id: int,
        data: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Update an organization field."""
        url = f"{self.base_url}/organization_fields/{field_id}"
        response = self._request('PUT', url, data={'organization_field': data})
        return response.json().get('organization_field', {})

    def get_organization_field_options(self, field_id: int) -> List[Dict]:
        """Get fresh options for an organization field."""
        field = self.get_organization_field(field_id)
        return field.get('custom_field_options', [])

    def update_organization_field_option_via_field(
        self,
        field_id: int,
        option_value: str,
        new_name: str
    ) -> Dict[str, Any]:
        """Update an organization field option by updating the entire field."""
        field = self.get_organization_field(field_id)
        options = field.get('custom_field_options', [])

        if not options:
            raise Exception(f"Organization field {field_id} has no options")

        found = False
        updated_options = []
        for opt in options:
            if opt.get('value') == option_value:
                updated_options.append({
                    'name': new_name,
                    'value': opt.get('value')
                })
                found = True
            else:
                updated_options.append({
                    'name': opt.get('raw_name', opt.get('name', '')),
                    'value': opt.get('value')
                })

        if not found:
            raise Exception(
                f"Option with value '{option_value}' not found in "
                f"organization field {field_id}"
            )

        return self.update_organization_field(
            field_id,
            {'custom_field_options': updated_options}
        )

    def update_organization_field_option(
        self,
        field_id: int,
        option_id: int,
        data: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Update an organization field option (legacy method)."""
        url = (
            f"{self.base_url}/organization_fields/{field_id}"
            f"/options/{option_id}"
        )
        response = self._request('PUT', url, data={
            'custom_field_option': data
        })
        return response.json().get('custom_field_option', {})

    # =========================================================================
    # GROUPS
    # =========================================================================

    def get_groups(self) -> List[Dict]:
        """Get all groups."""
        url = f"{self.base_url}/groups"
        return self._paginate(url, 'groups')

    def update_group(
        self,
        group_id: int,
        data: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Update a group."""
        url = f"{self.base_url}/groups/{group_id}"
        response = self._request('PUT', url, data={'group': data})
        return response.json().get('group', {})

    # =========================================================================
    # MACROS
    # =========================================================================

    def get_macros(self) -> List[Dict]:
        """Get all macros."""
        url = f"{self.base_url}/macros"
        return self._paginate(url, 'macros')

    def update_macro(
        self,
        macro_id: int,
        data: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Update a macro."""
        url = f"{self.base_url}/macros/{macro_id}"
        response = self._request('PUT', url, data={'macro': data})
        return response.json().get('macro', {})

    # =========================================================================
    # TRIGGERS
    # =========================================================================

    def get_triggers(self) -> List[Dict]:
        """Get all triggers."""
        url = f"{self.base_url}/triggers"
        return self._paginate(url, 'triggers')

    def update_trigger(
        self,
        trigger_id: int,
        data: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Update a trigger."""
        url = f"{self.base_url}/triggers/{trigger_id}"
        response = self._request('PUT', url, data={'trigger': data})
        return response.json().get('trigger', {})

    # =========================================================================
    # AUTOMATIONS
    # =========================================================================

    def get_automations(self) -> List[Dict]:
        """Get all automations."""
        url = f"{self.base_url}/automations"
        return self._paginate(url, 'automations')

    def update_automation(
        self,
        automation_id: int,
        data: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Update an automation."""
        url = f"{self.base_url}/automations/{automation_id}"
        response = self._request('PUT', url, data={'automation': data})
        return response.json().get('automation', {})

    # =========================================================================
    # VIEWS
    # =========================================================================

    def get_views(self) -> List[Dict]:
        """Get all views."""
        url = f"{self.base_url}/views"
        return self._paginate(url, 'views')

    def update_view(
        self,
        view_id: int,
        data: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Update a view."""
        url = f"{self.base_url}/views/{view_id}"
        response = self._request('PUT', url, data={'view': data})
        return response.json().get('view', {})

    # =========================================================================
    # SLA POLICIES
    # =========================================================================

    def get_sla_policies(self) -> List[Dict]:
        """Get all SLA policies."""
        url = f"{self.base_url}/slas/policies"
        response = self._request('GET', url)
        return response.json().get('sla_policies', [])

    def update_sla_policy(
        self,
        policy_id: int,
        data: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Update an SLA policy."""
        url = f"{self.base_url}/slas/policies/{policy_id}"
        response = self._request('PUT', url, data={'sla_policy': data})
        return response.json().get('sla_policy', {})

    # =========================================================================
    # HELP CENTER
    # =========================================================================

    def _check_help_center_enabled(self) -> bool:
        """Check if Help Center is enabled for this account."""
        try:
            # Try to access the Help Center API
            url = f"{self.hc_base_url}/categories"
            response = self.session.get(
                url,
                timeout=API_CONFIG.TIMEOUT_DEFAULT
            )
            # 404 means HC is not enabled or not set up
            return response.status_code != 404
        except Exception:
            return False

    def get_hc_categories(self) -> List[Dict]:
        """Get all Help Center categories."""
        # Try without locale first (works for single-locale accounts)
        url = f"{self.hc_base_url}/categories"
        try:
            return self._paginate(url, 'categories', retry_on_404=False)
        except requests.exceptions.HTTPError as e:
            if '404' in str(e):
                # Help Center not enabled or requires locale
                logger.debug(
                    "Help Center categories not accessible. "
                    "HC may not be enabled for this account."
                )
                return []
            raise

    def get_hc_sections(self) -> List[Dict]:
        """Get all Help Center sections."""
        url = f"{self.hc_base_url}/sections"
        try:
            return self._paginate(url, 'sections', retry_on_404=False)
        except requests.exceptions.HTTPError as e:
            if '404' in str(e):
                logger.debug(
                    "Help Center sections not accessible. "
                    "HC may not be enabled for this account."
                )
                return []
            raise

    def get_hc_articles(self) -> List[Dict]:
        """Get all Help Center articles."""
        url = f"{self.hc_base_url}/articles"
        try:
            return self._paginate(url, 'articles', retry_on_404=False)
        except requests.exceptions.HTTPError as e:
            if '404' in str(e):
                logger.debug(
                    "Help Center articles not accessible. "
                    "HC may not be enabled for this account."
                )
                return []
            raise