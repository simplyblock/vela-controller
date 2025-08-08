import re
from typing import Annotated

from fastapi import APIRouter
from fastapi.routing import APIRoute
from pydantic import Field, StringConstraints

Slug = Annotated[str, StringConstraints(
        pattern=r'^[a-zA-Z0-9]+(-[a-zA-Z0-9]+)*$',
        max_length=50,
)]

Int64 = Annotated[int, Field(ge=-2 ** 63, lt=2 ** 63)]


def single(xs):
    """Returns the single value in the passed collection

    If `xs` contains zero or multiple values, a ValueError error is raised.
    """

    it = iter(xs)

    try:
        x = next(it)
    except StopIteration:
        raise ValueError('No values present') from None

    try:
        next(it)
        raise ValueError('Multiple values present')
    except StopIteration:
        return x


def link_instance_creation(api: APIRouter, instance_api: APIRouter):
    """Links instance api endpoints to instance creation

    This expects `api` to have a POST endpoint at for '/', creating an instance that
    is accessed by endpoints defined at `instance_api`.
    More technically:
    - `api` has POST endpoint for '/', yielding a 201 response with a Location-header referencing the created entity
    - `instance_api` prefix contains single format-placeholder for the entity ID
      format-placeholder in its prefix that defines the ID of the entity
    - route names equal operationIDs
    """
    entity_id_name = single(re.findall(r'{(\w+)}', instance_api.prefix))
    creation_route = single(
            route for route in api.routes
            if isinstance(route, APIRoute) and route.path == '/' and route.methods == {'POST'}
    )

    assert(201 in creation_route.responses)
    assert(creation_route.responses[201].get('content') is None)
    assert(creation_route.responses[201].get('headers', {}).get('Location') is not None)

    creation_route.responses[201]['links'] = {
            route.name.split(':')[-1]: {
                'operationId': route.name,
                'parameters': {entity_id_name: '$response.header.Location#regex:/(.+)/'},
            }
            for route
            in instance_api.routes
            if isinstance(route, APIRoute)
    }
