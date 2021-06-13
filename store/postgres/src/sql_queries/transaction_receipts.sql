select
    decode(
        case when length(receipt ->> 'gasUsed') % 2 = 0 then
            ltrim(receipt ->> 'gasUsed', '0x')
        else
            replace((receipt ->> 'gasUsed'), 'x', '')
        end, 'hex') as gas_used,
    decode(replace(receipt ->> 'status', 'x', ''), 'hex') as status
from (
    select
        jsonb_array_elements(data -> 'transaction_receipts') as receipt
    from
        chain3.blocks
    where
        number = $1) as aliased;
