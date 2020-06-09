class DebugStep(object):
    """debug another step"""

    def __init__(self, step):
        self.step = step

    async def execute(self):
        print("PAUSING BEFORE EXECUTE")
        import pdb

        pdb.set_trace()

        value = await self.step.execute()

        print("PAUSING AFTER EXECUTE")
        import pdb

        pdb.set_trace()
        return value
