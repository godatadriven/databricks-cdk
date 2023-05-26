import {DockerImage} from "../../typescript/src";

describe("DockerImage", () => {
    test("DockerImage version check", () => {
        const imageVersion = DockerImage.version();

        const expectedVersion = `${require('../package.json').version}`;

        expect(imageVersion).toBe(expectedVersion);
    });
});
