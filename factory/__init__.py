def __getattr__(name):
    if name == "JobFactory":
        from factory.job_factory import JobFactory
        return JobFactory
    if name == "TableFactory":
        from factory.table_factory import TableFactory
        return TableFactory
    if name == "UploadScriptFactory":
        from factory.upload_script_factory import UploadScriptFactory
        return UploadScriptFactory
    if name == "Raw2FinalScriptFactory":
        from factory.raw2final_script_factory import Raw2FinalScriptFactory
        return Raw2FinalScriptFactory
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


__all__ = ["JobFactory", "TableFactory", "UploadScriptFactory", "Raw2FinalScriptFactory"]
