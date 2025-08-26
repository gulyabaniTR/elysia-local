from fastapi import APIRouter, Depends, Body
from fastapi.responses import JSONResponse

from elysia.api.dependencies.common import get_user_manager
from elysia.api.services.user import UserManager

# Logging
from elysia.api.core.log import logger

router = APIRouter()


@router.get("/{user_id}/saved_trees")
async def get_saved_trees(
    user_id: str,
    user_manager: UserManager = Depends(get_user_manager),
):

    headers = {"Cache-Control": "no-cache"}

    user = await user_manager.get_user_local(user_id)
    save_location_client_manager = user["frontend_config"].save_location_client_manager
    if not save_location_client_manager.is_client:
        logger.warning(
            "In /get_saved_trees API, "
            "no valid destination for trees location found. "
            "Returning no error but an empty list of trees."
        )
        return JSONResponse(
            content={"trees": {}, "error": ""},
            status_code=200,
            headers=headers,
        )

    try:
        trees = await user_manager.get_saved_trees(
            user_id, save_location_client_manager
        )
        return JSONResponse(
            content={"trees": trees, "error": ""}, status_code=200, headers=headers
        )

    except Exception as e:
        logger.error(f"Error getting saved trees: {str(e)}")
        return JSONResponse(
            content={"trees": {}, "error": str(e)}, status_code=500, headers=headers
        )


@router.get("/{user_id}/load_tree/{conversation_id}")
async def load_tree(
    user_id: str,
    conversation_id: str,
    user_manager: UserManager = Depends(get_user_manager),
):

    headers = {"Cache-Control": "no-cache"}

    try:
        frontend_rebuild = await user_manager.load_tree(user_id, conversation_id)
        return JSONResponse(
            content={"rebuild": frontend_rebuild, "error": ""},
            status_code=200,
            headers=headers,
        )
    except Exception as e:
        logger.error(f"Error loading tree: {str(e)}")
        return JSONResponse(
            content={"rebuild": [], "error": str(e)},
            status_code=500,
            headers=headers,
        )


@router.post("/{user_id}/save_tree/{conversation_id}")
async def save_tree(
    user_id: str,
    conversation_id: str,
    user_manager: UserManager = Depends(get_user_manager),
):
    try:
        await user_manager.save_tree(user_id, conversation_id)
        return JSONResponse(content={"error": ""}, status_code=200)
    except Exception as e:
        logger.error(f"Error saving tree: {str(e)}")
        return JSONResponse(content={"error": str(e)}, status_code=500)


@router.delete("/{user_id}/delete_tree/{conversation_id}")
async def delete_tree(
    user_id: str,
    conversation_id: str,
    user_manager: UserManager = Depends(get_user_manager),
):
    try:
        await user_manager.delete_tree(user_id, conversation_id)
        return JSONResponse(content={"error": ""}, status_code=200)
    except Exception as e:
        logger.error(f"Error deleting tree: {str(e)}")
        return JSONResponse(content={"error": str(e)}, status_code=500)


@router.post("/{user_id}/import/{collection_name}")
async def import_collection_data(
    user_id: str,
    collection_name: str,
    payload: dict = Body(...),
    user_manager: UserManager = Depends(get_user_manager),
):
    """
    Import JSON array payload into a Weaviate collection. Auto-creates schema if missing.

    Expected payload:
      {
        "items": [ { ...objects... } ]
      }

    Minimal normalization performed for arrays, year, age_limit, platform.
    """
    headers = {"Cache-Control": "no-cache"}

    try:
        user = await user_manager.get_user_local(user_id)
        client_manager = user["client_manager"]

        items = payload.get("items")
        if not isinstance(items, list) or len(items) == 0:
            return JSONResponse(
                content={"inserted": 0, "errors": ["items must be a non-empty list"], "error": ""},
                status_code=400,
                headers=headers,
            )

        # lightweight normalizer
        def norm_item(obj: dict) -> dict:
            x = dict(obj)
            # arrays
            for k in ["genres", "casts", "indexes"]:
                if k in x and isinstance(x[k], str):
                    x[k] = [s.strip() for s in x[k].split(",") if s.strip()]
            # platform nested
            if isinstance(x.get("platform"), dict):
                # flatten selected platform fields
                pf = x["platform"]
                if "name" in pf and "platform_name" not in x:
                    x["platform_name"] = pf.get("name")
            # year from date
            if "date" in x and isinstance(x["date"], str) and len(x["date"]) >= 4 and x.get("year") is None:
                try:
                    x["year"] = int(x["date"][0:4])
                except Exception:
                    pass
            # age limit mapping
            if "rtukRatingShort" in x and "ageLimit" not in x:
                x["ageLimit"] = str(x["rtukRatingShort"]).strip()
            return x

        norm_items = [norm_item(it) for it in items]

        # Insert
        async with client_manager.connect_to_async_client() as client:
            # Ensure collection exists (no vectorizer by default; inverted index timestamps enabled)
            import weaviate.classes.config as wc

            if not await client.collections.exists(collection_name):
                # Build a minimal flexible schema: create basic properties if common keys exist
                props = []
                common_text_fields = [
                    "name",
                    "description",
                    "type",
                    "channelGenre",
                    "poster",
                    "ageLimit",
                    "semantic_text",
                ]
                for p in common_text_fields:
                    props.append(wc.Property(name=p, data_type=wc.DataType.TEXT))
                # arrays
                for p in ["genres", "casts", "indexes"]:
                    props.append(wc.Property(name=p, data_type=wc.DataType.TEXT_ARRAY))
                # numbers
                for p in ["year", "contentId"]:
                    props.append(wc.Property(name=p, data_type=wc.DataType.NUMBER))
                # extra platform_name as TEXT
                props.append(wc.Property(name="platform_name", data_type=wc.DataType.TEXT))

                await client.collections.create(
                    collection_name,
                    vectorizer_config=wc.Configure.Vectorizer.none(),
                    inverted_index_config=wc.Configure.inverted_index(index_timestamps=True),
                    properties=props,
                )

            collection = client.collections.get(collection_name)
            result = await collection.data.insert_many(norm_items)

            inserted = 0
            errors: list[str] = []
            if hasattr(result, "has_errors") and result.has_errors:
                errors = [str(e) for e in result.errors] if getattr(result, "errors", None) else ["insert_many failed"]
            else:
                inserted = len(norm_items)

            return JSONResponse(
                content={"inserted": inserted, "errors": errors, "error": ""},
                status_code=200,
                headers=headers,
            )

    except Exception as e:
        logger.exception("Error importing data")
        return JSONResponse(
            content={"inserted": 0, "errors": [str(e)], "error": str(e)},
            status_code=500,
            headers=headers,
        )
