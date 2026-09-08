import {createVideoDetailApiFallback} from './videoDetailApiFallback.js';
import {validateFullCrawlYoutubeJsDetail} from './fullCrawlYoutubeJsModel.js';

export function createCheckpointYoutubeJsDetail({query,withTransaction,loadSettings,fetchDetail,
  fallback = createVideoDetailApiFallback({query,withTransaction,loadSettings})}) {
  return async (row,{signal=null}={}) => {
    try {
      const detail = await fallback({videoId:row.source_content_id,runId:row.run_id,
        requestId:JSON.stringify(['full',row.run_id,row.source_content_id]),consumer:'full',
        attempt:Number(row.attempts??0)+1,optionalComments:true,signal,
        fetch:()=>fetchDetail(row.source_content_id,{signal,strictRequiredSurfaces:true,
          optionalComments:true,detailMode:'full',requireContentType:true}),
        validate:detail=>validateFullCrawlYoutubeJsDetail(row.source_content_id,detail,{optionalComments:true}).detail});
      return {detail,error:null};
    }catch(error){return {detail:null,error};}
  };
}
