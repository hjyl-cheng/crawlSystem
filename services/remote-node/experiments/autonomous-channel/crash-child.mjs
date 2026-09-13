import { collectChannel } from './channel.mjs';
import { Journal } from './journal.mjs';
import { inputFixture, youtubeFixture } from './fixture.mjs';

await collectChannel({ input: inputFixture(), journal: await new Journal(process.argv[2]).init(),
  youtube: youtubeFixture(), afterCheckpoint: async key => {
    if (key === 'detail:first_seen:new-a') process.kill(process.pid, 'SIGKILL');
  } });
