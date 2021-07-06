use super::error::ExplorerError;
use byteorder::{BigEndian, LittleEndian};
use chain_core::property::{Block as _, Fragment as _, Serialize};
use chain_impl_mockchain::{
    certificate::Certificate,
    config::ConfigParam,
    fragment::Fragment,
    header::HeaderId,
    transaction::{self, InputEnum, Witness},
    value::Value,
};
use sanakirja::{btree, direct_repr, Commit, LoadPage, RootDb, Storable, UnsizedStorable};
use std::{convert::TryInto, fmt, mem::size_of, path::Path, sync::Arc};
use zerocopy::{
    byteorder::{U32, U64},
    AsBytes, FromBytes, Unaligned,
};

#[derive(Debug, thiserror::Error)]
#[error(transparent)]
pub struct TxnErr<E: std::error::Error + 'static>(pub E);

#[derive(Debug, Clone, PartialEq, Eq, AsBytes, FromBytes)]
#[repr(transparent)]
pub struct B32(U32<BigEndian>);

#[derive(Debug, Clone, PartialEq, Eq, AsBytes, FromBytes)]
#[repr(transparent)]
pub struct L32(U32<LittleEndian>);

#[derive(Debug, Clone, Copy, PartialEq, Eq, AsBytes, FromBytes)]
#[repr(transparent)]
pub struct B64(U64<BigEndian>);

#[derive(Debug, Clone, PartialEq, Eq, AsBytes, FromBytes)]
#[repr(transparent)]
pub struct L64(U64<LittleEndian>);

impl L64 {
    pub fn new(n: u64) -> Self {
        Self(U64::<LittleEndian>::new(n))
    }

    pub fn get(&self) -> u64 {
        self.0.get()
    }
}

impl B64 {
    pub fn new(n: u64) -> Self {
        Self(U64::<BigEndian>::new(n))
    }

    pub fn get(&self) -> u64 {
        self.0.get()
    }
}

impl B32 {
    pub fn new(n: u32) -> Self {
        Self(U32::<BigEndian>::new(n))
    }

    pub fn get(&self) -> u32 {
        self.0.get()
    }
}

impl L32 {
    pub fn new(n: u32) -> Self {
        Self(U32::<LittleEndian>::new(n))
    }

    pub fn get(&self) -> u32 {
        self.0.get()
    }
}

impl AsRef<U64<LittleEndian>> for L64 {
    fn as_ref(&self) -> &U64<LittleEndian> {
        &self.0
    }
}

impl Ord for B64 {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.0.as_bytes().cmp(other.0.as_bytes())
    }
}

impl PartialOrd for B64 {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.0.as_bytes().partial_cmp(other.0.as_bytes())
    }
}

impl Ord for B32 {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.0.as_bytes().cmp(&other.0.as_bytes())
    }
}

impl PartialOrd for B32 {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.0.as_bytes().partial_cmp(other.0.as_bytes())
    }
}

impl Ord for L64 {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.0.get().cmp(&other.0.get())
    }
}

impl PartialOrd for L64 {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.0.get().partial_cmp(&other.0.get())
    }
}

impl Ord for L32 {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.0.get().cmp(&other.0.get())
    }
}

impl PartialOrd for L32 {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.0.get().partial_cmp(&other.0.get())
    }
}

direct_repr!(B32);
direct_repr!(L32);
direct_repr!(B64);
direct_repr!(L64);

#[derive(Debug, Clone, Ord, PartialOrd, Eq, PartialEq)]
#[repr(C)]
pub struct StakePoolMeta {
    pub registration: FragmentId,
    pub retirement: Option<FragmentId>,
}

direct_repr!(StakePoolMeta);

pub type SlotId = B32;
pub type EpochNumber = B32;

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, AsBytes, FromBytes)]
#[repr(C)]
pub struct BlockDate {
    pub epoch: EpochNumber,
    pub slot_id: SlotId,
}

impl From<chain_impl_mockchain::block::BlockDate> for BlockDate {
    fn from(d: chain_impl_mockchain::block::BlockDate) -> Self {
        Self {
            epoch: B32::new(d.epoch),
            slot_id: B32::new(d.slot_id),
        }
    }
}

pub type ChainLength = B32;

#[derive(Debug, PartialEq, PartialOrd, Eq, Ord)]
#[repr(C)]
pub struct BlockMeta {
    chain_length: ChainLength,
    date: BlockDate,
    parent_hash: BlockId,
    total_input: L64,
    total_output: L64,
}

direct_repr!(BlockMeta);

pub type PoolId = StorableHash;

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord)]
#[repr(C)]
pub struct BlockProducer {
    bytes: [u8; 32],
}

direct_repr!(BlockProducer);

#[derive(Debug, PartialEq, Eq, Ord, PartialOrd, AsBytes)]
#[repr(C)]
pub struct VotePlanMeta {
    pub vote_start: BlockDate,
    pub vote_end: BlockDate,
    pub committee_end: BlockDate,
    pub payload_type: PayloadType,
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, Ord, PartialOrd, Hash, AsBytes)]
#[repr(u8)]
pub enum PayloadType {
    Public = 1,
    Private = 2,
}

impl From<chain_impl_mockchain::vote::PayloadType> for PayloadType {
    fn from(p: chain_impl_mockchain::vote::PayloadType) -> Self {
        match p {
            chain_impl_mockchain::vote::PayloadType::Public => PayloadType::Public,
            chain_impl_mockchain::vote::PayloadType::Private => PayloadType::Private,
        }
    }
}

direct_repr!(VotePlanMeta);

pub type ExternalProposalId = StorableHash;
pub type Options = u8;

#[derive(Debug, FromBytes, AsBytes, PartialEq, Eq, Ord, PartialOrd)]
#[repr(C)]
pub struct ExplorerVoteProposal {
    pub proposal_id: ExternalProposalId,
    pub options: Options,
    // pub tally: Option<ExplorerVoteTally>,
}

impl From<&chain_impl_mockchain::certificate::Proposal> for ExplorerVoteProposal {
    fn from(p: &chain_impl_mockchain::certificate::Proposal) -> Self {
        ExplorerVoteProposal {
            proposal_id: StorableHash::from(<[u8; 32]>::from(p.external_id().clone())),
            options: Options::from(p.options().choice_range().end),
        }
    }
}

direct_repr!(ExplorerVoteProposal);

// TODO do proper vote tally
// #[derive(Clone)]
// pub enum ExplorerVoteTally {
//     Public {
//         results: Box<[Weight]>,
//         options: Options,
//     },
//     Private {
//         results: Option<Vec<Weight>>,
//         options: Options,
//     },
// }

// impl ExplorerAddress {
//     pub fn to_single_account(&self) -> Option<Identifier> {
//         match self {
//             ExplorerAddress::New(address) => match address.kind() {
//                 chain_addr::Kind::Single(key) => Some(key.clone().into()),
//                 _ => None,
//             },
//             ExplorerAddress::Old(_) => None,
//         }
//     }
// }

pub(crate) type P<K, V> = btree::page::Page<K, V>;
type Db<K, V> = btree::Db<K, V>;
pub(crate) type UP<K, V> = btree::page_unsized::Page<K, V>;
type UDb<K, V> = btree::Db_<K, V, UP<K, V>>;

#[derive(Debug, thiserror::Error)]
pub enum SanakirjaError {
    #[error(transparent)]
    Sanakirja(#[from] ::sanakirja::Error),
    #[error("Pristine locked")]
    PristineLocked,
    #[error("Pristine corrupt")]
    PristineCorrupt,
    #[error("version error")]
    Version,
}

impl std::convert::From<::sanakirja::CRCError> for SanakirjaError {
    fn from(_: ::sanakirja::CRCError) -> Self {
        SanakirjaError::PristineCorrupt
    }
}

impl std::convert::From<::sanakirja::CRCError> for TxnErr<SanakirjaError> {
    fn from(_: ::sanakirja::CRCError) -> Self {
        TxnErr(SanakirjaError::PristineCorrupt)
    }
}

impl std::convert::From<::sanakirja::Error> for TxnErr<SanakirjaError> {
    fn from(e: ::sanakirja::Error) -> Self {
        TxnErr(e.into())
    }
}

impl std::convert::From<TxnErr<::sanakirja::Error>> for TxnErr<SanakirjaError> {
    fn from(e: TxnErr<::sanakirja::Error>) -> Self {
        TxnErr(e.0.into())
    }
}

// A Sanakirja pristine.
#[derive(Clone)]
pub struct Pristine {
    pub env: Arc<::sanakirja::Env>,
}

impl Pristine {
    pub fn new<P: AsRef<Path>>(name: P) -> Result<Self, SanakirjaError> {
        Self::new_with_size(name, 1 << 20)
    }
    pub unsafe fn new_nolock<P: AsRef<Path>>(name: P) -> Result<Self, SanakirjaError> {
        Self::new_with_size_nolock(name, 1 << 20)
    }
    pub fn new_with_size<P: AsRef<Path>>(name: P, size: u64) -> Result<Self, SanakirjaError> {
        let env = ::sanakirja::Env::new(name, size, 2);
        match env {
            Ok(env) => Ok(Pristine { env: Arc::new(env) }),
            Err(::sanakirja::Error::IO(e)) => {
                if let std::io::ErrorKind::WouldBlock = e.kind() {
                    Err(SanakirjaError::PristineLocked)
                } else {
                    Err(SanakirjaError::Sanakirja(::sanakirja::Error::IO(e)))
                }
            }
            Err(e) => Err(SanakirjaError::Sanakirja(e)),
        }
    }
    pub unsafe fn new_with_size_nolock<P: AsRef<Path>>(
        name: P,
        size: u64,
    ) -> Result<Self, SanakirjaError> {
        Ok(Pristine {
            env: Arc::new(::sanakirja::Env::new_nolock(name, size, 2)?),
        })
    }
    pub fn new_anon() -> Result<Self, SanakirjaError> {
        Self::new_anon_with_size(1 << 20)
    }
    pub fn new_anon_with_size(size: u64) -> Result<Self, SanakirjaError> {
        Ok(Pristine {
            env: Arc::new(::sanakirja::Env::new_anon(size, 2)?),
        })
    }
}

#[derive(Debug, AsBytes, FromBytes)]
#[repr(C)]
pub struct Stability {
    epoch_stability_depth: L32,
    last_stable_block: ChainLength,
}

#[derive(Debug, AsBytes, FromBytes)]
#[repr(C)]
pub struct BooleanStaticSettings {
    discrimination: L32,
    consensus: L32,
}

impl BooleanStaticSettings {
    pub fn new() -> Self {
        Self {
            discrimination: L32::new(0),
            consensus: L32::new(0),
        }
    }

    pub fn set_discrimination(&mut self, d: chain_addr::Discrimination) {
        match d {
            chain_addr::Discrimination::Production => self.discrimination = L32::new(1),
            chain_addr::Discrimination::Test => self.discrimination = L32::new(2),
        }
    }

    pub fn get_discrimination(&self) -> Option<chain_addr::Discrimination> {
        match self.discrimination.get() {
            0 => None,
            1 => Some(chain_addr::Discrimination::Production),
            2 => Some(chain_addr::Discrimination::Test),
            _ => unreachable!("invalid discrimination tag"),
        }
    }
    pub fn set_consensus(&mut self, c: chain_impl_mockchain::chaintypes::ConsensusType) {
        match c {
            chain_impl_mockchain::chaintypes::ConsensusType::Bft => self.consensus = L32::new(1),
            chain_impl_mockchain::chaintypes::ConsensusType::GenesisPraos => {
                self.consensus = L32::new(2)
            }
        }
    }

    pub fn get_consensus(&self) -> Option<chain_impl_mockchain::chaintypes::ConsensusType> {
        match self.consensus.get() {
            0 => None,
            1 => Some(chain_impl_mockchain::chaintypes::ConsensusType::Bft),
            2 => Some(chain_impl_mockchain::chaintypes::ConsensusType::GenesisPraos),
            _ => unreachable!("invalid discrimination tag"),
        }
    }
}

impl Default for BooleanStaticSettings {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, PartialEq, Clone, Copy)]
#[repr(usize)]
pub enum Root {
    Stability,
    BooleanStaticSettings,
    Blocks,
    VotePlans,
    VotePlanProposals,
    Transactions,
    ChainLenghts,
    Tips,
    StakePoolData,
    States,
}

impl Pristine {
    pub fn txn_begin(&self) -> Result<Txn, SanakirjaError> {
        let txn = ::sanakirja::Env::txn_begin(self.env.clone())?;
        fn begin(txn: ::sanakirja::Txn<Arc<::sanakirja::Env>>) -> Option<Txn> {
            Some(Txn {
                states: txn.root_db(Root::States as usize)?,
                tips: txn.root_db(Root::Tips as usize)?,
                chain_lengths: txn.root_db(Root::ChainLenghts as usize)?,
                transactions: txn.root_db(Root::Transactions as usize)?,
                blocks: txn.root_db(Root::Blocks as usize)?,
                vote_plans: txn.root_db(Root::VotePlans as usize)?,
                vote_plan_proposals: txn.root_db(Root::VotePlanProposals as usize)?,
                stake_pool_data: txn.root_db(Root::StakePoolData as usize)?,
                txn,
            })
        }
        if let Some(txn) = begin(txn) {
            Ok(txn)
        } else {
            Err(SanakirjaError::PristineCorrupt)
        }
    }

    pub fn mut_txn_begin(&self) -> Result<MutTxn<()>, SanakirjaError> {
        let mut txn = ::sanakirja::Env::mut_txn_begin(self.env.clone()).unwrap();
        Ok(MutTxn {
            states: if let Some(db) = txn.root_db(Root::States as usize) {
                db
            } else {
                btree::create_db_(&mut txn)?
            },
            tips: if let Some(db) = txn.root_db(Root::Tips as usize) {
                db
            } else {
                btree::create_db_(&mut txn)?
            },
            chain_lengths: if let Some(db) = txn.root_db(Root::ChainLenghts as usize) {
                db
            } else {
                btree::create_db_(&mut txn)?
            },
            transactions: if let Some(db) = txn.root_db(Root::Transactions as usize) {
                db
            } else {
                btree::create_db_(&mut txn)?
            },
            blocks: if let Some(db) = txn.root_db(Root::Blocks as usize) {
                db
            } else {
                btree::create_db_(&mut txn)?
            },
            vote_plans: if let Some(db) = txn.root_db(Root::VotePlans as usize) {
                db
            } else {
                btree::create_db_(&mut txn)?
            },
            vote_plan_proposals: if let Some(db) = txn.root_db(Root::VotePlanProposals as usize) {
                db
            } else {
                btree::create_db_(&mut txn)?
            },
            stake_pool_data: if let Some(db) = txn.root_db(Root::StakePoolData as usize) {
                db
            } else {
                btree::create_db_(&mut txn)?
            },
            txn,
        })
    }
}

pub type Txn = GenericTxn<::sanakirja::Txn<Arc<::sanakirja::Env>>>;
pub type MutTxn<T> = GenericTxn<::sanakirja::MutTxn<Arc<::sanakirja::Env>, T>>;

pub type Transactions = UDb<Pair<FragmentId, TxComponentTag>, TxComponent>;

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord)]
#[repr(u8)]
pub enum TxComponentTag {
    Input = 0,
    Output,
    Certificate,
}

direct_repr!(TxComponentTag);

const fn max(a: usize, b: usize) -> usize {
    if a > b {
        a
    } else {
        b
    }
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord)]
#[repr(C)]
pub struct TxComponent(
    [u8; max(
        std::mem::size_of::<TransactionInput>(),
        max(
            std::mem::size_of::<TransactionOutput>(),
            std::mem::size_of::<TransactionCertificate>(),
        ),
    )],
);

impl TxComponent {
    unsafe fn as_output(&self) -> &TransactionOutput {
        std::mem::transmute(self.0.as_ptr() as *const TransactionOutput)
    }

    unsafe fn as_input(&self) -> &TransactionInput {
        std::mem::transmute(self.0.as_ptr() as *const TransactionInput)
    }

    unsafe fn as_tagged_certificate(&self) -> &TransactionCertificate {
        std::mem::transmute(self.0.as_ptr() as *const TransactionCertificate)
    }

    pub fn from_input(i: TransactionInput) -> Self {
        let mut alloc = [0u8; size_of::<Self>()];
        let bytes = i.as_bytes();
        alloc.copy_from_slice(&bytes);

        Self(alloc)
    }

    pub fn from_output(i: TransactionOutput) -> Self {
        let mut alloc = [0u8; size_of::<Self>()];
        let bytes = i.as_bytes();
        alloc.copy_from_slice(&bytes);

        Self(alloc)
    }

    pub fn from_tagged_certificate(i: TransactionCertificate) -> Self {
        let mut alloc = [0u8; size_of::<Self>()];
        let bytes = i.as_bytes();
        alloc.copy_from_slice(&bytes);

        Self(alloc)
    }
}

direct_repr!(TxComponent);

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, AsBytes, FromBytes)]
#[repr(C)]
pub struct TransactionInput {
    pub input_ptr: [u8; 32],
    pub value: L64,
    pub utxo_or_account: u8,
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord)]
#[repr(u8)]
enum InputType {
    Utxo = 0x00,
    Account = 0xff,
}

impl From<&transaction::Input> for TransactionInput {
    fn from(i: &transaction::Input) -> Self {
        TransactionInput {
            input_ptr: i.bytes()[9..].try_into().unwrap(),
            utxo_or_account: match i.get_type() {
                transaction::InputType::Utxo => InputType::Utxo as u8,
                transaction::InputType::Account => InputType::Account as u8,
            },
            value: L64::new(i.value().0),
        }
    }
}

direct_repr!(TransactionInput);

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, AsBytes, FromBytes)]
#[repr(C)]
pub struct TransactionOutput {
    pub idx: u8,
    pub address: Address,
    pub value: L64,
}

impl TransactionOutput {
    pub fn from_original(idx: u8, output: &transaction::Output<chain_addr::Address>) -> Self {
        TransactionOutput {
            idx,
            address: Address::from(output.address.clone()),
            value: L64::new(output.value.0),
        }
    }
}

direct_repr!(TransactionOutput);

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, AsBytes)]
#[repr(C)]
pub struct TransactionCertificate {
    tag: CertificateTag,
    cert: SerializedCertificate,
}

impl TransactionCertificate {
    const fn alloc() -> [u8; size_of::<SerializedCertificate>()] {
        [0u8; size_of::<SerializedCertificate>()]
    }

    pub fn from_vote_plan_meta(meta: VotePlanMeta) -> Self {
        let mut alloc = [0u8; size_of::<SerializedCertificate>()];
        alloc.copy_from_slice(meta.as_bytes());

        TransactionCertificate {
            tag: CertificateTag::VotePlan,
            cert: SerializedCertificate(alloc),
        }
    }

    pub fn from_public_vote_cast(vote: PublicVoteCast) -> Self {
        let mut alloc = Self::alloc();
        alloc.copy_from_slice(vote.as_bytes());

        TransactionCertificate {
            tag: CertificateTag::VoteCast,
            cert: SerializedCertificate(alloc),
        }
    }
}

direct_repr!(TransactionCertificate);

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, AsBytes)]
#[repr(u8)]
enum CertificateTag {
    VotePlan = 0,
    VoteCast = 1,
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, AsBytes)]
#[repr(C)]
pub struct SerializedCertificate(
    [u8; max(
        std::mem::size_of::<VotePlanMeta>(),
        std::mem::size_of::<PublicVoteCast>(),
    )],
);

pub type Choice = u8;

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, AsBytes)]
#[repr(C)]
pub struct PublicVoteCast {
    vote_plan: VotePlanId,
    proposal_index: u8,
    payload: Choice,
}

pub type Blocks = Db<BlockId, BlockMeta>;
pub type BlockTransactions = Db<BlockId, FragmentId>;
pub type ChainLengths = Db<ChainLength, BlockId>;
pub type VotePlans = UDb<VotePlanId, VotePlanMeta>;
pub type VotePlanProposals = UDb<VotePlanId, Pair<u8, ExplorerVoteProposal>>;
pub type StakePools = UDb<PoolId, StakePoolMeta>;
pub type Tips = Db<Pair<B32, BlockId>, ()>;

// multiverse
pub type States = Db<BlockId, SerializedStateRef>;

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord)]
#[repr(C)]
pub struct AccountId([u8; chain_impl_mockchain::transaction::INPUT_PTR_SIZE]);
direct_repr!(AccountId);

pub type ProposalIndex = u8;
pub type ProposalId = Pair<VotePlanId, ProposalIndex>;
pub type StakeControl = Db<AccountId, Stake>;
pub type StakePoolBlocks = Db<PoolIdRecord, BlockId>;
pub type BlocksInBranch = Db<ChainLength, BlockId>;

pub type AddressId = SeqNum;
pub type AddressIds = Db<Address, AddressId>;
pub type AddressTransactions = Db<AddressId, Pair<SeqNum, FragmentId>>;
// TODO: this could be the actual vote instead of the FragmentId
pub type Votes = Db<ProposalId, Pair<SeqNum, Choice>>;

// a strongly typed (and in-memory) version of SerializedStateRef
pub struct StateRef {
    stake_pool_blocks: StakePoolBlocks,
    stake_control: StakeControl,
    blocks: BlocksInBranch,
    address_id: AddressIds,
    addresses: AddressTransactions,
    votes: Votes,
    next_address_id: Option<SeqNum>,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
#[repr(C)]
pub struct SerializedStateRef {
    pub stake_pool_blocks: L64,
    pub stake_control: L64,
    pub blocks: L64,
    pub address_id: L64,
    pub addresses: L64,
    pub votes: L64,
}

type BlockId = StorableHash;

impl Into<HeaderId> for BlockId {
    fn into(self) -> HeaderId {
        HeaderId::from(self.0)
    }
}

type FragmentId = StorableHash;
type VotePlanId = StorableHash;

#[derive(Debug, Clone, PartialOrd, Ord, PartialEq, Eq, AsBytes, FromBytes)]
#[repr(C)]
pub struct StorableHash([u8; 32]);

direct_repr!(StorableHash);

impl StorableHash {
    const MAX: Self = StorableHash([0xff; 32]);
}

impl From<chain_impl_mockchain::key::Hash> for StorableHash {
    fn from(id: chain_impl_mockchain::key::Hash) -> Self {
        let bytes: [u8; 32] = id.into();

        Self(bytes)
    }
}

impl From<[u8; 32]> for StorableHash {
    fn from(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }
}

pub struct GenericTxn<T: ::sanakirja::LoadPage<Error = ::sanakirja::Error> + ::sanakirja::RootPage>
{
    #[doc(hidden)]
    pub txn: T,

    pub states: States,
    pub tips: Tips,
    pub chain_lengths: ChainLengths,
    pub transactions: Transactions,
    pub blocks: Blocks,
    pub vote_plans: VotePlans,
    pub vote_plan_proposals: VotePlanProposals,
    pub stake_pool_data: StakePools,
}

impl<T: ::sanakirja::LoadPage<Error = ::sanakirja::Error> + ::sanakirja::RootPage> GenericTxn<T> {}

#[derive(Debug, Clone, Copy, PartialOrd, Ord, PartialEq, Eq)]
#[repr(C)]
pub struct Pair<A, B> {
    pub a: A,
    pub b: B,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
#[repr(C)]
pub struct SeqNum(B64);

direct_repr!(SeqNum);

impl SeqNum {
    pub const MAX: SeqNum = SeqNum(B64(U64::<BigEndian>::MAX_VALUE));
    pub const MIN: SeqNum = SeqNum(B64(U64::<BigEndian>::ZERO));

    pub fn new(n: u64) -> Self {
        Self(B64::new(n))
    }

    pub fn next(self) -> SeqNum {
        Self::new(self.0.get() + 1)
    }
}

pub type Stake = L64;
pub type PoolIdRecord = Pair<PoolId, SeqNum>;

const MAX_ADDRESS_SIZE: usize = chain_addr::ADDR_SIZE_GROUP;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, AsBytes, FromBytes)]
#[repr(C)]
pub struct Address([u8; MAX_ADDRESS_SIZE]);

impl Address {
    const MAX: Address = Address([255u8; MAX_ADDRESS_SIZE]);
}

direct_repr!(Address);

impl From<chain_addr::Address> for Address {
    fn from(addr: chain_addr::Address) -> Self {
        let mut bytes = [0u8; MAX_ADDRESS_SIZE];
        addr.serialize(&mut bytes[..]).unwrap();
        Self(bytes)
    }
}

impl From<&chain_addr::Address> for Address {
    fn from(addr: &chain_addr::Address) -> Self {
        let mut bytes = [0u8; MAX_ADDRESS_SIZE];
        addr.serialize(&mut bytes[..]).unwrap();
        Self(bytes)
    }
}

impl TryInto<chain_addr::Address> for Address {
    type Error = chain_addr::Error;

    fn try_into(self) -> Result<chain_addr::Address, Self::Error> {
        chain_addr::Address::from_bytes(self.0.as_ref())
    }
}

impl StateRef {
    pub fn new_empty<T>(txn: &mut T) -> Self
    where
        T: ::sanakirja::AllocPage
            + ::sanakirja::LoadPage<Error = ::sanakirja::Error>
            + ::sanakirja::RootPage,
    {
        Self {
            stake_pool_blocks: btree::create_db_(txn).unwrap(),
            stake_control: btree::create_db_(txn).unwrap(),
            blocks: btree::create_db_(txn).unwrap(),
            address_id: btree::create_db_(txn).unwrap(),
            addresses: btree::create_db_(txn).unwrap(),
            votes: btree::create_db_(txn).unwrap(),

            next_address_id: None,
        }
    }

    pub fn finish(mut self, txn: &mut SanakirjaMutTx) -> SerializedStateRef {
        // if the sequence counter for addresses was incremented previously, rewrite it
        if let Some(next_seq) = self.next_address_id {
            btree::del(txn, &mut self.address_id, &Address([0u8; 65]), None).unwrap();

            btree::put(
                txn,
                &mut self.address_id,
                &Address([0u8; 65]),
                &next_seq.next(),
            )
            .unwrap();
        }

        // TODO: update address counter
        SerializedStateRef {
            stake_pool_blocks: L64::new(self.stake_pool_blocks.db),
            stake_control: L64::new(self.stake_control.db),
            blocks: L64::new(self.blocks.db),
            address_id: L64::new(self.address_id.db),
            addresses: L64::new(self.addresses.db),
            votes: L64::new(self.votes.db),
        }
    }

    pub fn apply_vote(
        &mut self,
        txn: &mut SanakirjaMutTx,
        vote_plan_id: &VotePlanId,
        proposal_index: u8,
        choice: Choice,
    ) -> Result<(), ExplorerError> {
        let proposal_id = Pair {
            a: vote_plan_id.clone(),
            b: proposal_index,
        };

        let max_possible_value = Pair {
            a: SeqNum::MAX,
            b: u8::MAX,
        };

        let seq = find_last_record_by(txn, &self.votes, &proposal_id, &max_possible_value)
            .map(|last| last.a.next())
            .unwrap_or(SeqNum::MIN);

        btree::put(
            txn,
            &mut self.votes,
            &proposal_id,
            &Pair { a: seq, b: choice },
        )
        .unwrap();

        Ok(())
    }

    pub fn add_transaction_to_address(
        &mut self,
        txn: &mut SanakirjaMutTx,
        fragment_id: &FragmentId,
        address: &Address,
    ) -> Result<(), ExplorerError> {
        let address_id = self.get_or_insert_address_id(txn, address);

        let max_possible_value = Pair {
            a: SeqNum::MAX,
            b: FragmentId::MAX,
        };

        let seq = find_last_record_by(&*txn, &self.addresses, &address_id, &max_possible_value)
            .map(|last| last.a.next())
            .unwrap_or(SeqNum::MIN);

        btree::put(
            txn,
            &mut self.addresses,
            &address_id,
            &Pair {
                a: seq,
                b: fragment_id.clone(),
            },
        )
        .unwrap();

        Ok(())
    }

    pub fn add_block_to_blocks(
        &mut self,
        txn: &mut SanakirjaMutTx,
        chain_length: &ChainLength,
        block_id: &BlockId,
    ) -> Result<(), ExplorerError> {
        btree::put(txn, &mut self.blocks, chain_length, block_id).unwrap();
        Ok(())
    }

    fn get_or_insert_address_id(&mut self, txn: &SanakirjaMutTx, address: &Address) -> SeqNum {
        let address_exists = btree::get(txn, &self.address_id, address, None)
            .unwrap()
            .map(|(_, v)| v)
            .cloned();
        let address_id = if let Some(v) = address_exists {
            v
        } else {
            let next_seq = if let Some(next_seq) = self.next_address_id {
                next_seq
            } else {
                *btree::get(txn, &self.address_id, &Address([0u8; 65]), None)
                    .unwrap()
                    .unwrap()
                    .1
            };

            self.next_address_id = Some(next_seq.next());

            next_seq
        };
        address_id
    }

    pub fn apply_output_to_stake_control(
        &mut self,
        txn: &mut SanakirjaMutTx,
        output: &transaction::Output<chain_addr::Address>,
    ) -> Result<(), ExplorerError> {
        match output.address.kind() {
            chain_addr::Kind::Single(account) => {
                self.add_stake_to_account(txn, account, output.value);
            }
            chain_addr::Kind::Group(_, account) => {
                self.add_stake_to_account(txn, account, output.value);
            }
            chain_addr::Kind::Account(_) => {}
            chain_addr::Kind::Multisig(_) => {}
            chain_addr::Kind::Script(_) => {}
        }
        Ok(())
    }

    fn add_stake_to_account(
        &mut self,
        txn: &mut SanakirjaMutTx,
        account: &chain_crypto::PublicKey<chain_crypto::Ed25519>,
        value: Value,
    ) {
        let op =
            |current_stake: u64, value: u64| -> u64 { current_stake.checked_add(value).unwrap() };

        self.update_stake_for_account(txn, account, op, value);
    }

    fn substract_stake_from_account(
        &mut self,
        txn: &mut SanakirjaMutTx,
        account: &chain_crypto::PublicKey<chain_crypto::Ed25519>,
        value: Value,
    ) {
        let op =
            |current_stake: u64, value: u64| -> u64 { current_stake.checked_sub(value).unwrap() };

        self.update_stake_for_account(txn, account, op, value);
    }

    fn update_stake_for_account(
        &mut self,
        txn: &mut SanakirjaMutTx,
        account: &chain_crypto::PublicKey<chain_crypto::Ed25519>,
        op: impl Fn(u64, u64) -> u64,
        value: Value,
    ) {
        let account_id = AccountId(account.as_ref().try_into().unwrap());

        let current_stake = btree::get(txn, &self.stake_control, &account_id, None)
            .unwrap()
            .map(|(_, stake)| stake.get())
            .unwrap_or(0);

        let new_stake = op(current_stake, value.0);

        btree::del(txn, &mut self.stake_control, &account_id, None).unwrap();
        btree::put(
            txn,
            &mut self.stake_control,
            &account_id,
            &L64::new(new_stake),
        )
        .unwrap();
    }
}

type SanakirjaMutTx = ::sanakirja::MutTxn<Arc<::sanakirja::Env>, ()>;
type SanakirjaTx = ::sanakirja::Txn<Arc<::sanakirja::Env>>;

impl SerializedStateRef {
    pub fn fork(&self, txn: &mut SanakirjaMutTx) -> StateRef {
        StateRef {
            stake_pool_blocks: btree::fork_db(txn, &Db::from_page(self.stake_pool_blocks.get()))
                .unwrap(),
            stake_control: btree::fork_db(txn, &Db::from_page(self.stake_control.get())).unwrap(),
            blocks: btree::fork_db(txn, &Db::from_page(self.blocks.get())).unwrap(),
            address_id: btree::fork_db(txn, &Db::from_page(self.address_id.get())).unwrap(),
            addresses: btree::fork_db(txn, &Db::from_page(self.addresses.get())).unwrap(),
            votes: btree::fork_db(txn, &Db::from_page(self.votes.get())).unwrap(),
            next_address_id: None,
        }
    }
}

direct_repr!(SerializedStateRef);

impl MutTxn<()> {
    pub fn initialize(
        &mut self,
        block_id: HeaderId,
        initial: impl Iterator<Item = Fragment>,
    ) -> Result<(), ExplorerError> {
        let block_id = StorableHash::from(block_id);
        let empty_state = StateRef::new_empty(&mut self.txn);
        let new_state = &empty_state.finish(&mut self.txn);

        let Self {
            txn,
            states,
            tips,
            chain_lengths,
            transactions,
            blocks,
            vote_plans,
            vote_plan_proposals,
            stake_pool_data,
        } = self;

        btree::put(txn, states, &block_id, &new_state).unwrap();

        Ok(())
    }

    pub fn add_block(
        &mut self,
        parent_id: BlockId,
        block_id: BlockId,
        chain_length: u32,
        block_date: BlockDate,
        fragments: impl Iterator<Item = Fragment>,
    ) -> Result<(), ExplorerError> {
        let block_chain_length = ChainLength::new(chain_length);

        let states = btree::get(&self.txn, &self.states, &parent_id, None)
            .unwrap()
            .filter(|(branch_id, _states)| *branch_id == &parent_id)
            .map(|(_branch_id, states)| states)
            .cloned()
            .ok_or_else(|| ExplorerError::AncestorNotFound(block_id.clone().into()))?;

        let mut state_ref = states.fork(&mut self.txn);

        for fragment in fragments {
            let fragment_id = StorableHash::from(fragment.id());
            match &fragment {
                Fragment::Initial(config_params) => {
                    let mut settings = BooleanStaticSettings::new();
                    let mut stability: Stability = unsafe {
                        std::mem::transmute(self.txn.root(Root::Stability as usize).unwrap())
                    };

                    for config_param in config_params.iter() {
                        match config_param {
                            ConfigParam::Discrimination(d) => {
                                settings.set_discrimination(*d);
                            }
                            ConfigParam::Block0Date(_) => {}
                            ConfigParam::ConsensusVersion(c) => {
                                settings.set_consensus(*c);
                            }
                            ConfigParam::SlotsPerEpoch(_) => {}
                            ConfigParam::SlotDuration(_) => {}
                            ConfigParam::EpochStabilityDepth(c) => {
                                stability.set_epoch_stability_depth(c);
                            }
                            ConfigParam::ConsensusGenesisPraosActiveSlotsCoeff(_) => {}
                            ConfigParam::BlockContentMaxSize(_) => {}
                            ConfigParam::AddBftLeader(_) => {}
                            ConfigParam::RemoveBftLeader(_) => {}
                            ConfigParam::LinearFee(_) => {}
                            ConfigParam::ProposalExpiration(_) => {}
                            ConfigParam::KesUpdateSpeed(_) => {}
                            ConfigParam::TreasuryAdd(_) => {}
                            ConfigParam::TreasuryParams(_) => {}
                            ConfigParam::RewardPot(_) => {}
                            ConfigParam::RewardParams(_) => {}
                            ConfigParam::PerCertificateFees(_) => {}
                            ConfigParam::FeesInTreasury(_) => {}
                            ConfigParam::RewardLimitNone => {}
                            ConfigParam::RewardLimitByAbsoluteStake(_) => {}
                            ConfigParam::PoolRewardParticipationCapping(_) => {}
                            ConfigParam::AddCommitteeId(_) => {}
                            ConfigParam::RemoveCommitteeId(_) => {}
                            ConfigParam::PerVoteCertificateFees(_) => {}
                        }
                    }
                }
                Fragment::OldUtxoDeclaration(_) => {}
                Fragment::Transaction(tx) => {
                    self.apply_transaction(fragment_id, &tx, &mut state_ref)?;
                }
                Fragment::OwnerStakeDelegation(tx) => {
                    self.apply_transaction(fragment_id, &tx, &mut state_ref)?;
                }
                Fragment::StakeDelegation(tx) => {
                    self.apply_transaction(fragment_id, &tx, &mut state_ref)?;
                }
                Fragment::PoolRegistration(tx) => {
                    self.apply_transaction(fragment_id, &tx, &mut state_ref)?;
                }
                Fragment::PoolRetirement(tx) => {
                    self.apply_transaction(fragment_id, &tx, &mut state_ref)?;
                }
                Fragment::PoolUpdate(tx) => {
                    self.apply_transaction(fragment_id, &tx, &mut state_ref)?;
                }
                Fragment::UpdateProposal(_) => {}
                Fragment::UpdateVote(_) => {}
                Fragment::VotePlan(tx) => {
                    self.apply_transaction(fragment_id, &tx, &mut state_ref)?;
                    self.add_vote_plan_meta(tx);
                }
                Fragment::VoteCast(tx) => {
                    self.apply_transaction(fragment_id.clone(), &tx, &mut state_ref)?;

                    let vote_cast = tx.as_slice().payload().into_payload();
                    let vote_plan_id =
                        StorableHash(<[u8; 32]>::from(vote_cast.vote_plan().clone()));

                    let proposal_index = vote_cast.proposal_index();
                    match vote_cast.payload() {
                        chain_impl_mockchain::vote::Payload::Public { choice } => {
                            state_ref.apply_vote(
                                &mut self.txn,
                                &vote_plan_id,
                                proposal_index,
                                choice.as_byte(),
                            )?;
                        }
                        // private vote not supported yet
                        chain_impl_mockchain::vote::Payload::Private {
                            encrypted_vote: _,
                            proof: _,
                        } => (),
                    }
                }
                Fragment::VoteTally(tx) => {
                    self.apply_transaction(fragment_id, &tx, &mut state_ref)?;
                }
                Fragment::EncryptedVoteTally(tx) => {
                    self.apply_transaction(fragment_id, &tx, &mut state_ref)?;
                }
            }

            self.update_stake_pool_meta(&fragment)?;
        }

        state_ref.add_block_to_blocks(&mut self.txn, &ChainLength::new(chain_length), &block_id)?;

        let new_state = state_ref.finish(&mut self.txn);

        if !btree::put(&mut self.txn, &mut self.states, &block_id, &new_state).unwrap() {
            return Err(ExplorerError::BlockAlreadyExists(block_id.into()));
        }

        self.update_tips(&parent_id, block_chain_length.clone(), &block_id)?;

        self.update_chain_lengths(block_chain_length.clone(), &block_id)?;

        self.add_block_meta(
            block_id,
            BlockMeta {
                chain_length: B32::new(block_chain_length.get()),
                date: block_date,
                parent_hash: parent_id,
                total_input: L64::new(0),
                total_output: L64::new(0),
            },
        )?;

        Ok(())
    }

    fn add_vote_plan_meta(
        &mut self,
        tx: &transaction::Transaction<chain_impl_mockchain::certificate::VotePlan>,
    ) {
        let vote_plan = tx.as_slice().payload().into_payload();
        let vote_plan_id = StorableHash(<[u8; 32]>::from(vote_plan.to_id()));
        let vote_plan_meta = VotePlanMeta {
            vote_start: vote_plan.vote_start().into(),
            vote_end: vote_plan.vote_end().into(),
            committee_end: vote_plan.committee_end().into(),
            payload_type: vote_plan.payload_type().into(),
        };

        for (idx, proposal) in vote_plan.proposals().iter().enumerate() {
            btree::put(
                &mut self.txn,
                &mut self.vote_plan_proposals,
                &vote_plan_id,
                &Pair {
                    a: idx as u8,
                    b: proposal.into(),
                },
            )
            .unwrap();
        }

        btree::put(
            &mut self.txn,
            &mut self.vote_plans,
            &vote_plan_id,
            &vote_plan_meta,
        )
        .unwrap();
    }

    fn apply_transaction<P>(
        &mut self,
        fragment_id: FragmentId,
        tx: &transaction::Transaction<P>,
        state: &mut StateRef,
    ) -> Result<(), ExplorerError> {
        let etx = tx.as_slice();

        // is important that we put outputs first, because utxo's can refer to inputs in the same
        // transaction, so those need to be already indexed. Although it would be technically
        // faster to just look for them in the serialized transaction, that's increases complexity
        // for something that is not really that likely. Besides, the pages should be in the system
        // cache because we recently inserted them.
        for (idx, output) in etx.outputs().iter().enumerate() {
            self.put_transaction_output(fragment_id.clone(), idx as u8, &output)
                .unwrap();
            state.apply_output_to_stake_control(&mut self.txn, &output)?;
            state.add_transaction_to_address(
                &mut self.txn,
                &fragment_id,
                &output.address.into(),
            )?;
        }

        for (input, witness) in etx.inputs_and_witnesses().iter() {
            self.put_transaction_input(fragment_id.clone(), &input)
                .unwrap();

            let resolved_utxo = match input.to_enum() {
                InputEnum::AccountInput(_, _) => None,
                InputEnum::UtxoInput(input) => Some(self.resolve_utxo(&self.transactions, input)),
            };

            self.apply_input_to_stake_control(&input, &witness, resolved_utxo.as_ref(), state)?;

            self.apply_input_to_transactions_by_address(
                &fragment_id,
                &input,
                &witness,
                resolved_utxo.as_ref(),
                state,
            )?;
        }

        Ok(())
    }

    pub fn update_tips(
        &mut self,
        parent_id: &BlockId,
        chain_length: ChainLength,
        block_id: &BlockId,
    ) -> Result<(), ExplorerError> {
        let parent_key = Pair {
            a: B32::new(chain_length.get() - 1),
            b: parent_id.clone(),
        };

        let _ = btree::del(&mut self.txn, &mut self.tips, &parent_key, None).unwrap();

        let key = Pair {
            a: B32::new(chain_length.get()),
            b: block_id.clone(),
        };

        btree::put(&mut self.txn, &mut self.tips, &key, &()).unwrap();

        Ok(())
    }

    pub fn update_chain_lengths(
        &mut self,
        chain_length: ChainLength,
        block_id: &BlockId,
    ) -> Result<(), ExplorerError> {
        btree::put(
            &mut self.txn,
            &mut self.chain_lengths,
            &chain_length,
            block_id,
        )
        .unwrap();

        Ok(())
    }

    pub fn put_transaction_input(
        &mut self,
        fragment_id: FragmentId,
        input: &transaction::Input,
    ) -> Result<(), ExplorerError> {
        btree::put(
            &mut self.txn,
            &mut self.transactions,
            &Pair {
                a: fragment_id,
                b: TxComponentTag::Input,
            },
            &TxComponent::from_input(TransactionInput::from(input)),
        )
        .unwrap();

        Ok(())
    }

    pub fn put_transaction_output(
        &mut self,
        fragment_id: FragmentId,
        idx: u8,
        output: &transaction::Output<chain_addr::Address>,
    ) -> Result<(), ExplorerError> {
        btree::put(
            &mut self.txn,
            &mut self.transactions,
            &Pair {
                a: fragment_id,
                b: TxComponentTag::Output,
            },
            &TxComponent::from_output(TransactionOutput::from_original(idx, output)),
        )
        .unwrap();

        Ok(())
    }

    pub fn put_transaction_certificate(
        &mut self,
        fragment_id: FragmentId,
        cert: TransactionCertificate,
    ) -> Result<(), ExplorerError> {
        btree::put(
            &mut self.txn,
            &mut self.transactions,
            &Pair {
                a: fragment_id,
                b: TxComponentTag::Certificate,
            },
            &TxComponent::from_tagged_certificate(cert),
        )
        .unwrap();

        Ok(())
    }

    pub fn update_stake_pool_meta(&mut self, fragment: &Fragment) -> Result<(), ExplorerError> {
        match fragment {
            Fragment::PoolRegistration(tx) => {
                let etx = tx.as_slice();
                let cert = etx.payload();

                let stake_pool_id = match cert.into_certificate_slice().unwrap().into_owned() {
                    Certificate::PoolRegistration(r) => r.to_id(),
                    _ => unreachable!("mismatched certificate type"),
                };

                btree::put(
                    &mut self.txn,
                    &mut self.stake_pool_data,
                    &StorableHash(<[u8; 32]>::from(stake_pool_id)),
                    &StakePoolMeta {
                        registration: StorableHash::from(fragment.id()),
                        retirement: None,
                    },
                )
                .unwrap();
            }
            Fragment::PoolRetirement(tx) => {
                let etx = tx.as_slice();
                let cert = etx.payload();

                let stake_pool_id = match cert.into_certificate_slice().unwrap().into_owned() {
                    Certificate::PoolRetirement(r) => r.pool_id,
                    _ => unreachable!("mismatched certificate type"),
                };

                let stake_pool_id = StorableHash(<[u8; 32]>::from(stake_pool_id));

                let mut new = btree::get(&self.txn, &self.stake_pool_data, &stake_pool_id, None)
                    .unwrap()
                    .map(|(_, meta)| meta)
                    .cloned()
                    .unwrap();

                new.retirement = Some(FragmentId::from(fragment.id()));

                btree::del(
                    &mut self.txn,
                    &mut self.stake_pool_data,
                    &stake_pool_id,
                    None,
                )
                .unwrap();

                btree::put(
                    &mut self.txn,
                    &mut self.stake_pool_data,
                    &stake_pool_id,
                    &new,
                )
                .unwrap();
            }
            _ => {}
        }

        Ok(())
    }

    pub fn add_block_meta(
        &mut self,
        block_id: BlockId,
        block: BlockMeta,
    ) -> Result<(), ExplorerError> {
        btree::put(&mut self.txn, &mut self.blocks, &block_id, &block).unwrap();

        Ok(())
    }

    pub fn apply_input_to_stake_control(
        &mut self,
        input: &transaction::Input,
        witness: &transaction::Witness,
        resolved_utxo: Option<&TransactionOutput>,
        state: &mut StateRef,
    ) -> Result<(), ExplorerError> {
        match (input.to_enum(), witness) {
            (InputEnum::AccountInput(account, value), Witness::Account(_)) => {
                state.substract_stake_from_account(
                    &mut self.txn,
                    account.to_single_account().unwrap().as_ref(),
                    value,
                );
            }
            (InputEnum::AccountInput(_, _), Witness::Multisig(_)) => {}
            (InputEnum::UtxoInput(_), Witness::Utxo(_)) => {
                // TODO: this is not the cleanest way of doing this...
                let output = resolved_utxo.expect("missing utxo pointer resolution");

                let address: chain_addr::Address = output.address.clone().try_into().unwrap();

                if let chain_addr::Kind::Group(_, account) = address.kind() {
                    let value = &output.value;
                    state.substract_stake_from_account(&mut self.txn, &account, Value(value.get()));
                }
            }
            (InputEnum::UtxoInput(_), Witness::OldUtxo(_, _, _)) => {}
            _ => unreachable!("invalid combination of input and witness"),
        }
        Ok(())
    }

    pub fn apply_input_to_transactions_by_address(
        &mut self,
        fragment_id: &FragmentId,
        input: &transaction::Input,
        witness: &transaction::Witness,
        resolved_utxo: Option<&TransactionOutput>,
        state: &mut StateRef,
    ) -> Result<(), ExplorerError> {
        match (input.to_enum(), witness) {
            (InputEnum::AccountInput(account_id, _value), Witness::Account(_)) => {
                let kind = chain_addr::Kind::Account(
                    account_id
                        .to_single_account()
                        .expect("the input to be validated")
                        .into(),
                );

                // TODO: get this from the block0
                let discrimination = chain_addr::Discrimination::Test;
                let address = chain_addr::Address(discrimination, kind).into();

                state.add_transaction_to_address(&mut self.txn, &fragment_id, &address)?;
            }
            (InputEnum::AccountInput(_, _), Witness::Multisig(_)) => {}
            (InputEnum::UtxoInput(_), Witness::Utxo(_)) => {
                // TODO: this is not the cleanest way of doing this...
                let output = resolved_utxo.expect("missing utxo pointer resolution");

                state.add_transaction_to_address(
                    &mut self.txn,
                    &fragment_id,
                    &output.address.clone(),
                )?;
            }
            (InputEnum::UtxoInput(_), Witness::OldUtxo(_, _, _)) => {}
            _ => unreachable!("invalid combination of input and witness"),
        }

        Ok(())
    }

    fn resolve_utxo(
        &self,
        transactions: &Transactions,
        utxo_pointer: transaction::UtxoPointer,
    ) -> TransactionOutput {
        let txid = utxo_pointer.transaction_id;
        let idx = utxo_pointer.output_index;

        let mut cursor = btree::Cursor::new(&self.txn, &transactions).unwrap();

        cursor
            .set(
                &self.txn,
                &Pair {
                    a: StorableHash::from(txid),
                    b: TxComponentTag::Output,
                },
                Some(&TxComponent::from_output(TransactionOutput {
                    idx,
                    address: Address::MAX,
                    value: L64::new(u64::MAX),
                })),
            )
            .unwrap();

        if let Some((_, output)) = cursor.current(&self.txn).unwrap() {
            unsafe { output.as_output().clone() }
        } else {
            panic!("missing utxo")
        }
    }

    pub fn commit(self) {
        // destructure things so we get some sort of exhaustiveness-check
        let Self {
            mut txn,
            states,
            tips,
            chain_lengths,
            transactions,
            blocks,
            vote_plans,
            vote_plan_proposals,
            stake_pool_data,
        } = self;

        txn.set_root(Root::States as usize, states.db);
        txn.set_root(Root::Tips as usize, tips.db);
        txn.set_root(Root::ChainLenghts as usize, chain_lengths.db);
        txn.set_root(Root::Transactions as usize, transactions.db);
        txn.set_root(Root::Blocks as usize, blocks.db);
        txn.set_root(Root::VotePlans as usize, vote_plans.db);
        txn.set_root(Root::VotePlanProposals as usize, vote_plan_proposals.db);
        txn.set_root(Root::StakePoolData as usize, stake_pool_data.db);

        txn.commit().unwrap();
    }
}

impl Txn {
    pub fn get() {
        todo!();
    }
}

impl<A: Storable, B: Storable> Storable for Pair<A, B> {
    type PageReferences = core::iter::Chain<A::PageReferences, B::PageReferences>;
    fn page_references(&self) -> Self::PageReferences {
        self.a.page_references().chain(self.b.page_references())
    }
    fn compare<T: sanakirja::LoadPage>(&self, t: &T, b: &Self) -> core::cmp::Ordering {
        match self.a.compare(t, &b.a) {
            core::cmp::Ordering::Equal => self.b.compare(t, &b.b),
            ord => ord,
        }
    }
}

impl<A: Ord + UnsizedStorable, B: Ord + UnsizedStorable> UnsizedStorable for Pair<A, B> {
    const ALIGN: usize = std::mem::align_of::<(A, B)>();

    fn size(&self) -> usize {
        let a = self.a.size();
        let b_off = (a + (B::ALIGN - 1)) & !(B::ALIGN - 1);
        (b_off + self.b.size() + (Self::ALIGN - 1)) & !(Self::ALIGN - 1)
    }
    unsafe fn onpage_size(p: *const u8) -> usize {
        let a = A::onpage_size(p);
        let b_off = (a + (B::ALIGN - 1)) & !(B::ALIGN - 1);
        let b_size = B::onpage_size(p.add(b_off));
        (b_off + b_size + (Self::ALIGN - 1)) & !(Self::ALIGN - 1)
    }
    unsafe fn from_raw_ptr<'a, T>(_: &T, p: *const u8) -> &'a Self {
        &*(p as *const Self)
    }
    unsafe fn write_to_page(&self, p: *mut u8) {
        self.a.write_to_page(p);
        let off = (self.a.size() + (B::ALIGN - 1)) & !(B::ALIGN - 1);
        self.b.write_to_page(p.add(off));
    }
}

fn find_last_record_by<T, K, V>(
    txn: &T,
    tree: &Db<K, V>,
    key: &K,
    max_possible_value: &V,
) -> Option<V>
where
    K: Storable,
    V: Storable + Clone + PartialEq,
    T: ::sanakirja::LoadPage<Error = ::sanakirja::Error>,
{
    let mut cursor = btree::Cursor::new(txn, tree).unwrap();

    cursor.set(txn, key, Some(&max_possible_value)).unwrap();

    assert!(
        cursor
            .current(txn)
            .unwrap()
            .map(|(_, v)| v != max_possible_value)
            .unwrap_or(true),
        "ran out of sequence numbers"
    );

    cursor.prev(txn).unwrap();

    cursor.current(txn).unwrap().map(|(_, v)| v.clone())
}
